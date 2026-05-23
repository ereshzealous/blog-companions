package server

import (
	"context"
	"fmt"
	"strconv"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	vitalsv1 "github.com/wearable/device-gateway/proto/vitalsv1"
)

type Marshaler func(any) ([]byte, error)

type Server struct {
	vitalsv1.UnimplementedVitalsIngestServer
	writer  *kafka.Writer
	marshal Marshaler
	tracer  trace.Tracer
}

func New(writer *kafka.Writer, m Marshaler, tracer trace.Tracer) *Server {
	return &Server{writer: writer, marshal: m, tracer: tracer}
}

func (s *Server) PublishBatch(ctx context.Context, batch *vitalsv1.VitalsBatch) (*vitalsv1.PublishAck, error) {
	ctx, span := s.tracer.Start(ctx, "PublishBatch",
		trace.WithAttributes(
			attribute.String("device.id", batch.DeviceId),
			attribute.String("user.id", batch.UserId),
			attribute.Int("readings.count", len(batch.Readings)),
		),
	)
	defer span.End()

	if batch.DeviceId == "" || batch.UserId == "" {
		span.SetStatus(codes.Error, "missing device or user id")
		return &vitalsv1.PublishAck{Rejected: int32(len(batch.Readings)), Message: "missing ids"}, nil
	}

	readings := make([]map[string]any, 0, len(batch.Readings))
	for _, r := range batch.Readings {
		readings = append(readings, map[string]any{
			"ts_unix_ms":  r.TsUnixMs,
			"heart_rate":  r.HeartRate,
			"spo2":        r.Spo2,
			"accel":       map[string]float32{"x": r.AccelX, "y": r.AccelY, "z": r.AccelZ},
			"steps_delta": r.StepsDelta,
			"skin_temp_c": r.SkinTempC,
		})
	}

	// Default: ONE bundled Kafka message. Opt-in fan-out via the x-fanout-count
	// gRPC metadata splits the batch into N messages that all share this
	// PublishBatch's trace context — one real parent, N children.
	chunks := chunkReadings(readings, fanoutCount(ctx))

	for i, chunk := range chunks {
		payload := map[string]any{
			"device_id": batch.DeviceId,
			"user_id":   batch.UserId,
			"readings":  chunk,
		}
		body, err := s.marshal(payload)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("marshal: %w", err)
		}

		produceCtx, produceSpan := s.tracer.Start(ctx, "kafka.produce",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination.name", s.writer.Topic),
				attribute.Int("messaging.batch.message_count", len(chunks)),
				attribute.Int("messaging.batch.index", i),
				attribute.Int("readings.count", len(chunk)),
			),
		)

		carrier := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(produceCtx, carrier)
		headers := make([]kafka.Header, 0, len(carrier))
		for k, v := range carrier {
			headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
		}

		msg := kafka.Message{Key: []byte(batch.UserId), Value: body, Headers: headers}
		if err := s.writer.WriteMessages(produceCtx, msg); err != nil {
			produceSpan.SetStatus(codes.Error, err.Error())
			produceSpan.End()
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("kafka write: %w", err)
		}
		produceSpan.End()
	}

	span.SetAttributes(attribute.Int("messaging.batch.message_count", len(chunks)))
	span.SetStatus(codes.Ok, "")
	return &vitalsv1.PublishAck{Accepted: int32(len(batch.Readings)), Message: "ok"}, nil
}

// fanoutCount reads x-fanout-count from incoming gRPC metadata. <=1 or absent
// means a single bundled message.
func fanoutCount(ctx context.Context) int {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 1
	}
	vals := md.Get("x-fanout-count")
	if len(vals) == 0 {
		return 1
	}
	n, err := strconv.Atoi(vals[0])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// chunkReadings splits readings into n roughly-equal payloads. n<=1 returns one chunk.
func chunkReadings(readings []map[string]any, n int) [][]map[string]any {
	if n <= 1 || len(readings) <= 1 {
		return [][]map[string]any{readings}
	}
	if n > len(readings) {
		n = len(readings)
	}
	size := (len(readings) + n - 1) / n // ceil
	chunks := make([][]map[string]any, 0, n)
	for i := 0; i < len(readings); i += size {
		end := i + size
		if end > len(readings) {
			end = len(readings)
		}
		chunks = append(chunks, readings[i:end])
	}
	return chunks
}
