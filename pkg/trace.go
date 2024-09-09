package pkg

import "go.opentelemetry.io/otel"

var Tracer = otel.Tracer("broker")
