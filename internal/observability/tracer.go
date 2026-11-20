// internal/observability/tracer.go

package observability

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

// InitTracer initializes an OTLP gRPC trace exporter and registers it as the
// global TracerProvider. The Jaeger collector (v1.35+) natively accepts OTLP,
// so no Jaeger-specific SDK package is required.
//
// The exporter endpoint is read from the OTEL_EXPORTER_OTLP_ENDPOINT
// environment variable (default: "localhost:4317").
func InitTracer(serviceName string) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317"
	}

	exporter, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(
		context.Background(),
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithProcess(),
	)
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
	)

	// Register as the global TracerProvider so otel.Tracer() works everywhere.
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// TemporalTracingInterceptor returns a Temporal client interceptor that creates
// spans for starting workflows, running workflows, and running activities, and
// propagates trace context through Temporal headers so the API server's request,
// the workflow, and each activity land in a single trace. Call it after InitTracer.
func TemporalTracingInterceptor() (interceptor.ClientInterceptor, error) {
	return temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{})
}
