// Package otlp is the OTLP export client shared by the trace and log
// destination drivers: one configuration block (endpoint, protocol, TLS,
// headers, timeout, compression, retry), transport-neutral Span and LogRecord
// types with converters to the OTLP protobuf, a client that speaks OTLP/gRPC
// or OTLP/HTTP, and the error classification the OTLP specification defines.
//
// Constructing a client never touches the network: the gRPC connection is
// established on the first export and an unreachable endpoint surfaces as a
// retryable error from ExportSpans or ExportLogs, so a sink using this client
// starts degraded instead of failing startup.
//
// Partial-success responses are the documented exception to "nothing is
// dropped": the specification forbids retrying them, so rejected spans or
// records are a terminal loss for that destination. They are logged with the
// server message, counted in antwatcher_otlp_rejected_total{signal}, and the
// export is reported as success.
package otlp

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
)

// Protocols accepted in protocol.
const (
	ProtocolGRPC = "grpc"
	ProtocolHTTP = "http"
)

// Compressions accepted in compression.
const (
	CompressionNone = "none"
	CompressionGzip = "gzip"
)

// Config is the OTLP client block of a trace/otlp or log/otlp sink.
type Config struct {
	// Endpoint is the collector address. For grpc: host:port, optionally with an
	// http:// or https:// scheme. For http: host:port or a base URL
	// (http[s]://host:port[/path]); the client appends /v1/traces and /v1/logs.
	// An http:// scheme means plaintext, https:// means TLS; without a scheme
	// the connection uses TLS unless Insecure is true.
	Endpoint string `yaml:"endpoint" json:"endpoint"`
	// Protocol is grpc (default) or http (OTLP/HTTP with protobuf payloads).
	Protocol string `yaml:"protocol" json:"protocol"`
	// Insecure disables TLS for a scheme-less endpoint.
	Insecure bool `yaml:"insecure" json:"insecure"`
	// TLS configures the TLS client when TLS is in use.
	TLS TLSConfig `yaml:"tls" json:"tls"`
	// Headers are sent with every export (gRPC metadata or HTTP headers), for
	// example Authorization. Values are secrets and never rendered.
	Headers map[string]config.Secret `yaml:"headers" json:"headers,omitempty"`
	// Timeout bounds one export attempt.
	Timeout time.Duration `yaml:"timeout" json:"timeout"`
	// Compression is none (default) or gzip.
	Compression string `yaml:"compression" json:"compression"`
	// Retry bounds the short in-client retry of retryable failures; after that
	// the error is returned and the bus redelivers the event later.
	Retry RetryConfig `yaml:"retry" json:"retry"`
}

// TLSConfig configures the TLS client.
type TLSConfig struct {
	// CAFile is a PEM bundle that replaces the system roots when set.
	CAFile string `yaml:"ca_file" json:"ca_file"`
	// CertFile and KeyFile enable mutual TLS; both or neither.
	CertFile string `yaml:"cert_file" json:"cert_file"`
	KeyFile  string `yaml:"key_file" json:"key_file"`
	// ServerName overrides the name verified against the server certificate.
	ServerName string `yaml:"server_name" json:"server_name"`
}

// RetryConfig bounds the in-client retry of retryable failures.
type RetryConfig struct {
	// Attempts is the number of retries after the first attempt (0 disables).
	Attempts int `yaml:"attempts" json:"attempts"`
	// Backoff is the delay before the first retry; it doubles per retry. A
	// server-provided delay (gRPC RetryInfo, HTTP Retry-After) takes precedence.
	Backoff time.Duration `yaml:"backoff" json:"backoff"`
}

// DefaultConfig returns the documented defaults; Endpoint has none.
func DefaultConfig() Config {
	return Config{
		Protocol:    ProtocolGRPC,
		Timeout:     10 * time.Second,
		Compression: CompressionNone,
		Retry:       RetryConfig{Attempts: 2, Backoff: 500 * time.Millisecond},
	}
}

// Describe decodes the block strictly on top of DefaultConfig; see
// config.Describer. The result renders with Headers masked and implements
// config.DriverValidator so `-check` reports invalid blocks.
func Describe(raw yaml.Node) (any, error) {
	c := DefaultConfig()
	if err := config.DecodeStrict(raw, &c); err != nil {
		return nil, err
	}
	return c, nil
}

// Describer is Describe as a config.Describer.
var Describer config.Describer = config.DescriberFunc(Describe)

// ValidateWith implements config.DriverValidator; the OTLP client has no
// dependency on the router settings, so it is Validate.
func (c Config) ValidateWith(config.Router) error { return c.Validate() }

// Validate checks the block on its own.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.Endpoint == "" {
		add("endpoint is required")
	} else if tgt, err := c.target(); err != nil {
		add("endpoint: %w", err)
	} else if !tgt.tls && c.TLS != (TLSConfig{}) {
		add("tls settings are set but the endpoint is plaintext (insecure or http://)")
	}
	switch c.Protocol {
	case ProtocolGRPC, ProtocolHTTP:
	default:
		add("protocol %q must be %s or %s", c.Protocol, ProtocolGRPC, ProtocolHTTP)
	}
	switch c.Compression {
	case CompressionNone, CompressionGzip:
	default:
		add("compression %q must be %s or %s", c.Compression, CompressionNone, CompressionGzip)
	}
	if c.Timeout <= 0 {
		add("timeout must be > 0")
	}
	if c.Retry.Attempts < 0 {
		add("retry.attempts must be >= 0")
	}
	if c.Retry.Backoff < 0 {
		add("retry.backoff must be >= 0")
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		add("tls.cert_file and tls.key_file must be set together")
	}
	for k := range c.Headers {
		if strings.TrimSpace(k) == "" {
			add("headers: empty header name")
		}
	}
	return errors.Join(errs...)
}

// target is the parsed endpoint: host:port, whether TLS is used, and the base
// URL path for OTLP/HTTP.
type target struct {
	hostPort string
	tls      bool
	basePath string
}

// target resolves Endpoint against Insecure. A scheme, when present, decides
// TLS on its own; a contradiction with Insecure is an error.
func (c Config) target() (target, error) {
	ep := c.Endpoint
	if !strings.Contains(ep, "://") {
		if strings.ContainsAny(ep, "/?#") {
			return target{}, fmt.Errorf("%q: a path requires a scheme (http:// or https://)", ep)
		}
		if _, _, err := net.SplitHostPort(ep); err != nil {
			return target{}, fmt.Errorf("%q must be host:port or a URL", ep)
		}
		return target{hostPort: ep, tls: !c.Insecure}, nil
	}
	u, err := url.Parse(ep)
	if err != nil {
		return target{}, err
	}
	var useTLS bool
	switch u.Scheme {
	case "http":
		useTLS = false
	case "https":
		if c.Insecure {
			return target{}, fmt.Errorf("%q uses https but insecure is true", ep)
		}
		useTLS = true
	default:
		return target{}, fmt.Errorf("%q: scheme must be http or https", ep)
	}
	if u.Host == "" {
		return target{}, fmt.Errorf("%q has no host", ep)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return target{}, fmt.Errorf("%q must not carry credentials, a query, or a fragment", ep)
	}
	hostPort := u.Host
	if u.Port() == "" {
		port := "80"
		if useTLS {
			port = "443"
		}
		hostPort = net.JoinHostPort(u.Hostname(), port)
	}
	return target{hostPort: hostPort, tls: useTLS, basePath: strings.TrimSuffix(u.Path, "/")}, nil
}
