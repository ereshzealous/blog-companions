// loadgen simulates wearable devices publishing vitals to device-gateway over
// gRPC. A background loop drives DEVICE_COUNT devices on a fixed interval
// (periodically anomalous); POST /simulate triggers a single on-demand upload.
package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	vitalsv1 "github.com/wearable/loadgen/proto/vitalsv1"
)

const serviceName = "loadgen"

type device struct {
	id     string
	userID string
}

func newDevice() device {
	return device{id: uuid.NewString(), userID: uuid.NewString()}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func initTracer(ctx context.Context) (*sdktrace.TracerProvider, error) {
	endpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	res, _ := sdkresource.New(ctx, sdkresource.WithAttributes(
		semconv.ServiceName(serviceName),
	))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return tp, nil
}

func batch(d device, anomalous bool) *vitalsv1.VitalsBatch {
	now := time.Now().UnixMilli()
	readings := make([]*vitalsv1.Reading, 30)
	hrBase := float32(60 + rand.Intn(30))
	if anomalous {
		hrBase = float32(190 + rand.Intn(20)) // triggers tachycardia rule
	}
	for i := range readings {
		readings[i] = &vitalsv1.Reading{
			TsUnixMs:   now - int64((30-i)*1000),
			HeartRate:  hrBase + float32(rand.NormFloat64()*3),
			Spo2:       97 + float32(rand.NormFloat64()*0.5),
			AccelX:     float32(rand.NormFloat64() * 0.1),
			AccelY:     float32(rand.NormFloat64() * 0.1),
			AccelZ:     float32(rand.NormFloat64() * 0.1),
			StepsDelta: int32(rand.Intn(3)),
			SkinTempC:  33 + float32(rand.NormFloat64()*0.2),
		}
	}
	return &vitalsv1.VitalsBatch{
		DeviceId: d.id,
		UserId:   d.userID,
		Readings: readings,
	}
}

// publishOnce sends one batch under its own root span and returns the trace ID
// so callers can deep-link to it in Jaeger.
func publishOnce(ctx context.Context, client vitalsv1.VitalsIngestClient, d device, anomalous bool, trigger string, fanout int) (*vitalsv1.PublishAck, string, error) {
	tracer := otel.Tracer(serviceName)
	ctx, span := tracer.Start(ctx, "device.upload", oteltrace.WithAttributes(
		attribute.String("trigger", trigger),
		attribute.String("device.id", d.id),
		attribute.String("user.id", d.userID),
		attribute.Bool("anomalous", anomalous),
		attribute.Int("fanout", fanout),
	))
	defer span.End()

	// fanout>1 asks device-gateway to fan the batch into N messages, one trace.
	if fanout > 1 {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-fanout-count", strconv.Itoa(fanout))
	}

	ack, err := client.PublishBatch(ctx, batch(d, anomalous))
	if err != nil {
		span.RecordError(err)
	}
	return ack, span.SpanContext().TraceID().String(), err
}

func runDevice(ctx context.Context, client vitalsv1.VitalsIngestClient, d device, wg *sync.WaitGroup, interval time.Duration) {
	defer wg.Done()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	tickN := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			tickN++
			anomalous := tickN%20 == 0 // every 20th batch is intentionally anomalous
			ack, _, err := publishOnce(ctx, client, d, anomalous, "loop", 1)
			if err != nil {
				log.Printf("[%s] publish error: %v", d.id, err)
			} else {
				log.Printf("[%s] accepted=%d rejected=%d anomalous=%v", d.id, ack.Accepted, ack.Rejected, anomalous)
			}
		}
	}
}

type simulateResponse struct {
	DeviceID  string `json:"device_id"`
	UserID    string `json:"user_id"`
	Anomalous bool   `json:"anomalous"`
	Accepted  int32  `json:"accepted"`
	Rejected  int32  `json:"rejected"`
	TraceID   string `json:"trace_id"`
}

func startHTTP(ctx context.Context, addr string, client vitalsv1.VitalsIngestClient) {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})

	// POST /simulate?anomalous=&device_id=&user_id=&fanout=
	mux.HandleFunc("/simulate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		anomalous := q.Get("anomalous") == "true"
		fanout, _ := strconv.Atoi(q.Get("fanout")) // 0/absent → bundled single message

		d := newDevice()
		if id := q.Get("device_id"); id != "" {
			d.id = id
			if uid := q.Get("user_id"); uid != "" {
				d.userID = uid
			}
		}

		ack, traceID, err := publishOnce(r.Context(), client, d, anomalous, "manual", fanout)
		if err != nil {
			log.Printf("[simulate %s] publish error: %v", d.id, err)
			http.Error(w, "publish failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		log.Printf("[simulate %s] accepted=%d rejected=%d anomalous=%v trace=%s",
			d.id, ack.Accepted, ack.Rejected, anomalous, traceID)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(simulateResponse{
			DeviceID:  d.id,
			UserID:    d.userID,
			Anomalous: anomalous,
			Accepted:  ack.Accepted,
			Rejected:  ack.Rejected,
			TraceID:   traceID,
		})
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	log.Printf("simulate endpoint listening on %s (POST /simulate)", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("http server: %v", err)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp, err := initTracer(ctx)
	if err != nil {
		log.Fatalf("trace init: %v", err)
	}
	defer func() { _ = tp.Shutdown(context.Background()) }()

	target := getenv("DEVICE_GATEWAY", "device-gateway.wearable.svc.cluster.local:9000")
	interval, _ := time.ParseDuration(getenv("INTERVAL", "5s"))

	log.Printf("loadgen → %s every %s", target, interval)
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := vitalsv1.NewVitalsIngestClient(conn)

	go startHTTP(ctx, ":"+getenv("HTTP_PORT", "8080"), client)

	count, _ := strconv.Atoi(getenv("DEVICE_COUNT", "3"))
	if count < 1 {
		count = 1
	}
	devices := make([]device, count)
	for i := range devices {
		devices[i] = newDevice()
		log.Printf("device %s → user %s", devices[i].id, devices[i].userID)
	}

	var wg sync.WaitGroup
	for _, d := range devices {
		wg.Add(1)
		go runDevice(ctx, client, d, &wg, interval)
	}
	wg.Wait()
}
