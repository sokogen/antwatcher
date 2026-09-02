// Package otlp is the "otlp" driver of the trace class: it exports the spans
// projected by the trace package through the shared OTLP client
// (internal/otlp) over gRPC or HTTP.
//
// The driver registers itself as trace/otlp on import. Its configuration
// block is otlp.Config (endpoint, protocol, TLS, headers, timeout,
// compression, retry). Building the sink performs no network round trip: the
// connection is established on the first export, so an unreachable collector
// starts the sink degraded (retryable errors from Process, redelivered by
// the bus) instead of failing startup.
package otlp

import (
	"context"

	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
	otlpclient "github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/trace"
)

// Name is the driver name in configuration (sinks[].driver).
const Name = "otlp"

func init() {
	sink.RegisterDriver(sink.ClassTrace, Name, Driver())
}

// Driver returns the driver descriptor, for registration in custom registries.
func Driver() sink.Driver {
	return sink.Driver{Factory: Factory, Describer: otlpclient.Describer}
}

// Factory builds a trace sink over an OTLP client from raw, the sinks[].config
// block. It validates the block (unknown keys and invalid values fail) and
// reads TLS files, but never dials.
func Factory(_ context.Context, name string, raw yaml.Node, deps sink.Deps) (sink.Sink, error) {
	cfg := otlpclient.DefaultConfig()
	if err := config.DecodeStrict(raw, &cfg); err != nil {
		return nil, err
	}
	client, err := otlpclient.New(cfg, otlpclient.Options{
		Logger:  deps.Logger,
		Metrics: deps.Metrics,
		Version: deps.Version,
	})
	if err != nil {
		return nil, err
	}
	return trace.New(name, client), nil
}
