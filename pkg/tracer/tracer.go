package tracer

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	otrace "go.opentelemetry.io/otel/trace"
)

const _serviceName = "mapreduce"

var DefaultTracer = otel.Tracer(_serviceName)

var DefaultPropagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

func Init(endpoint string) error {
	headers := map[string]string{
		"content-type": "application/json",
	}

	exporter, err := otlptrace.New(
		context.Background(),
		otlptracehttp.NewClient(
			otlptracehttp.WithEndpoint(endpoint),
			otlptracehttp.WithHeaders(headers),
			otlptracehttp.WithInsecure(),
		),
	)
	if err != nil {
		return err
	}

	tracerProvider := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(_serviceName),
			),
		),
	)

	otel.SetTracerProvider(tracerProvider)

	return nil
}

func Start(ctx context.Context, spanName string, opts ...otrace.SpanStartOption) (context.Context, otrace.Span) {
	return DefaultTracer.Start(ctx, spanName, opts...)
}

func ApplySpan(mainCtx context.Context, ctxWithSpan context.Context) context.Context {
	span := otrace.SpanFromContext(mainCtx)
	mainCtx = otrace.ContextWithSpan(mainCtx, span)
	return mainCtx
}

func ToMap(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	DefaultPropagator.Inject(ctx, carrier)
	return carrier
}

func FromMap(ctx context.Context, m map[string]string) context.Context {
	return DefaultPropagator.Extract(ctx, propagation.MapCarrier(m))
}
