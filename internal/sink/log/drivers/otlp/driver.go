// Package otlp is the "otlp" driver of the log class: it exports the records
// projected by the log package through the shared OTLP client
// (internal/otlp) over gRPC or HTTP.
//
// The driver registers itself as log/otlp on import. Its configuration block
// is otlp.Config (endpoint, protocol, TLS, headers, timeout, compression,
// retry). Building the sink performs no network round trip: the connection is
// established on the first export, so an unreachable collector starts the
// sink degraded (retryable errors from Process, redelivered by the bus)
// instead of failing startup.
//
// Every record is exported on its own, synchronously: Write returns once the
// collector accepted the request, and the bus acks the message only then.
// There is no batch processor and nothing to flush at shutdown, so a crash
// never loses an acked record. Batching is a later optimisation behind the
// same log.Writer contract.
package otlp

import (
	"context"

	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
	otlpclient "github.com/sokogen/antwatcher/internal/otlp"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/log"
)

// Name is the driver name in configuration (sinks[].driver).
const Name = "otlp"

func init() {
	sink.RegisterDriver(sink.ClassLog, Name, Driver())
}

// Driver returns the driver descriptor, for registration in custom registries.
func Driver() sink.Driver {
	return sink.Driver{Factory: Factory, Describer: otlpclient.Describer}
}

// Factory builds a log sink over an OTLP client from raw, the sinks[].config
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
	return log.New(name, &writer{client: client}), nil
}

// writer adapts the OTLP client to the log.Writer contract: one record per
// export request.
type writer struct {
	client *otlpclient.Client
}

// Write exports rec in one request. The client's classification is passed
// through: permanent statuses come back wrapped by sink.Permanent, transport
// failures and retryable statuses stay retryable after the client's own
// short retries.
func (w *writer) Write(ctx context.Context, rec otlpclient.LogRecord) error {
	return w.client.ExportLogs(ctx, []otlpclient.LogRecord{rec})
}

// Close releases the client connection.
func (w *writer) Close() error { return w.client.Close() }
