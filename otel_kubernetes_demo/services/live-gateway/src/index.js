// live-gateway:
//   - WebSocket endpoint:  /ws?user_id=<uuid>
//   - REST endpoints:      POST /push-token, GET /healthz
//   - Kafka consumer of `alerts` topic — fans out to subscribed WebSockets
//
// Each alert fan-out creates a span LINKED to the upstream alert trace (via
// traceparent in Kafka headers). Long-lived WS connections are tracked as
// gauge metrics rather than per-frame spans to avoid span explosion.

import Fastify from 'fastify';
import websocket from '@fastify/websocket';
import { Kafka } from 'kafkajs';
import { trace, context, propagation, SpanKind } from '@opentelemetry/api';

const PORT = parseInt(process.env.PORT || '7000', 10);
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka-0.kafka.infra.svc.cluster.local:9092';
const ALERTS_TOPIC = process.env.ALERTS_TOPIC || 'alerts';

const tracer = trace.getTracer('live-gateway');

const subscribers = new Map(); // userId → Set<WebSocket>

const app = Fastify({ logger: true });
await app.register(websocket);

app.get('/healthz', async () => ({ status: 'ok' }));

app.post('/push-token', async (req, reply) => {
  // In a real system: persist the device push token (FCM/APNs).
  const { user_id, token } = req.body || {};
  if (!user_id || !token) return reply.code(400).send({ error: 'missing user_id/token' });
  return { user_id, registered: true };
});

app.get('/ws', { websocket: true }, (connection, req) => {
  const userId = req.query?.user_id;
  if (!userId) {
    connection.socket.send(JSON.stringify({ error: 'user_id required' }));
    connection.socket.close();
    return;
  }
  let set = subscribers.get(userId);
  if (!set) { set = new Set(); subscribers.set(userId, set); }
  set.add(connection.socket);
  app.log.info({ userId, subscriberCount: set.size }, 'ws connected');

  connection.socket.on('close', () => {
    set.delete(connection.socket);
    if (set.size === 0) subscribers.delete(userId);
    app.log.info({ userId }, 'ws disconnected');
  });
});

const kafka = new Kafka({ clientId: 'live-gateway', brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: 'live-gateway' });

async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: ALERTS_TOPIC, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      // Reconstruct upstream context from Kafka headers.
      const carrier = {};
      for (const [k, v] of Object.entries(message.headers || {})) {
        carrier[k] = v?.toString?.() ?? v;
      }
      const parentCtx = propagation.extract(context.active(), carrier);

      await context.with(parentCtx, async () => {
        const consumeSpan = tracer.startSpan('kafka.consume', {
          kind: SpanKind.CONSUMER,
          attributes: {
            'messaging.system': 'kafka',
            'messaging.destination.name': topic,
            'messaging.kafka.partition': partition,
            'messaging.kafka.offset': Number(message.offset),
          },
        });

        try {
          const alert = JSON.parse(message.value.toString());
          const userId = alert.user_id;
          const set = subscribers.get(userId);

          const pushSpan = tracer.startSpan('ws.push', {
            kind: SpanKind.PRODUCER,
            attributes: {
              'user.id': userId,
              'alert.id': alert.alert_id,
              'alert.severity': alert.severity,
              'alert.type': alert.type,
              'ws.subscribers': set ? set.size : 0,
            },
          });

          if (set && set.size > 0) {
            const payload = JSON.stringify({ type: 'alert', alert });
            for (const ws of set) {
              try { ws.send(payload); }
              catch (e) { app.log.error({ err: e }, 'ws send failed'); }
            }
          }
          pushSpan.end();
        } catch (e) {
          app.log.error({ err: e }, 'alert processing failed');
          consumeSpan.recordException(e);
        } finally {
          consumeSpan.end();
        }
      });
    },
  });
}

await app.listen({ host: '0.0.0.0', port: PORT });
app.log.info(`live-gateway listening on :${PORT}`);
startConsumer().catch((e) => {
  app.log.error({ err: e }, 'kafka consumer failed to start');
  process.exit(1);
});
