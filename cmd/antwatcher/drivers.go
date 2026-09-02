package main

// Every bus and sink driver registers itself with its default registry on
// import. The wiring in serve.go only ever talks to the registries, so this
// is the one place that knows which concrete drivers ship in the binary.
import (
	_ "github.com/sokogen/antwatcher/internal/bus/gochannel"
	_ "github.com/sokogen/antwatcher/internal/bus/natsjs"
	_ "github.com/sokogen/antwatcher/internal/sink/analytics/drivers/bigquery"
	_ "github.com/sokogen/antwatcher/internal/sink/archive/drivers/filesystem"
	_ "github.com/sokogen/antwatcher/internal/sink/forward/drivers/bus"
	_ "github.com/sokogen/antwatcher/internal/sink/log/drivers/otlp"
	_ "github.com/sokogen/antwatcher/internal/sink/log/drivers/stdout"
	_ "github.com/sokogen/antwatcher/internal/sink/trace/drivers/otlp"
)
