package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"strings"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/sokogen/antwatcher/internal/metrics"
)

// Signal labels of antwatcher_otlp_rejected_total.
const (
	SignalTraces = "traces"
	SignalLogs   = "logs"
)

// OTLP/HTTP paths.
const (
	tracesPath = "/v1/traces"
	logsPath   = "/v1/logs"
)

const (
	contentTypeProtobuf = "application/x-protobuf"
	maxResponseBytes    = 1 << 20
	maxErrorMessage     = 512
)

// Options are the dependencies of a Client.
type Options struct {
	// Logger receives partial-success and retry messages; nil discards them.
	Logger *slog.Logger
	// Metrics counts rejected items; nil counts nothing.
	Metrics *metrics.Metrics
	// Version is the antwatcher version: service.version, scope version, and
	// User-Agent. Empty means "dev".
	Version string
	// Resource overrides DefaultResource(Version) when its Attributes are set.
	Resource Resource
}

// Client exports spans and log records to one OTLP endpoint. It is safe for
// concurrent use.
type Client struct {
	cfg      Config
	endpoint string
	resource Resource
	logger   *slog.Logger
	metrics  *metrics.Metrics
	tr       transport
	// sleep waits for the retry delay; tests replace it.
	sleep func(ctx context.Context, d time.Duration) error
}

// transport is one OTLP wire protocol.
type transport interface {
	exportSpans(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error)
	exportLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error)
	close() error
}

// New validates cfg and builds the client. It reads TLS files but performs no
// network round trip: the connection is established on the first export, and
// an unreachable endpoint surfaces there as a retryable error.
func New(cfg Config, opts Options) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	tgt, err := cfg.target()
	if err != nil {
		return nil, fmt.Errorf("endpoint: %w", err)
	}
	var tlsCfg *tls.Config
	if tgt.tls {
		tlsCfg, err = cfg.TLS.build()
		if err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
	}
	version := opts.Version
	if version == "" {
		version = "dev"
	}
	c := &Client{
		cfg:      cfg,
		endpoint: cfg.Endpoint,
		resource: opts.Resource,
		logger:   opts.Logger,
		metrics:  opts.Metrics,
		sleep:    sleepCtx,
	}
	if c.resource.Attributes == nil {
		c.resource = DefaultResource(version)
	}
	if c.logger == nil {
		c.logger = slog.New(slog.DiscardHandler)
	}
	c.logger = c.logger.With("otlp_endpoint", cfg.Endpoint, "otlp_protocol", cfg.Protocol)
	userAgent := "antwatcher/" + version

	switch cfg.Protocol {
	case ProtocolGRPC:
		c.tr, err = newGRPCTransport(cfg, tgt, tlsCfg, userAgent)
	case ProtocolHTTP:
		c.tr = newHTTPTransport(cfg, tgt, tlsCfg, userAgent)
	default:
		err = fmt.Errorf("protocol %q", cfg.Protocol)
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Close releases the connection.
func (c *Client) Close() error { return c.tr.close() }

// ExportSpans exports spans in one request. Nil means every span was accepted
// or the server rejected some of them (partial success: logged, counted, not
// retried). Retryable failures are retried up to Retry.Attempts times and then
// returned as they are; permanent failures come back wrapped by
// sink.Permanent; a done ctx returns ctx.Err(). An empty slice is a no-op.
func (c *Client) ExportSpans(ctx context.Context, spans []Span) error {
	if len(spans) == 0 {
		return nil
	}
	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{ResourceSpans(c.resource, spans)},
	}
	return c.export(ctx, SignalTraces, func(ctx context.Context) (rejected int64, message string, err error) {
		resp, err := c.tr.exportSpans(ctx, req)
		if err != nil {
			return 0, "", err
		}
		ps := resp.GetPartialSuccess()
		return ps.GetRejectedSpans(), ps.GetErrorMessage(), nil
	})
}

// ExportLogs exports records in one request with the same contract as ExportSpans.
func (c *Client) ExportLogs(ctx context.Context, records []LogRecord) error {
	if len(records) == 0 {
		return nil
	}
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{ResourceLogs(c.resource, records)},
	}
	return c.export(ctx, SignalLogs, func(ctx context.Context) (rejected int64, message string, err error) {
		resp, err := c.tr.exportLogs(ctx, req)
		if err != nil {
			return 0, "", err
		}
		ps := resp.GetPartialSuccess()
		return ps.GetRejectedLogRecords(), ps.GetErrorMessage(), nil
	})
}

// attempt performs one export and reports the partial-success fields.
type attempt func(ctx context.Context) (rejected int64, message string, err error)

// export runs do with the configured timeout and retry policy.
func (c *Client) export(ctx context.Context, signal string, do attempt) error {
	var lastErr error
	for n := 0; ; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		rejected, message, err := c.once(ctx, do)
		if err == nil {
			if rejected > 0 {
				c.rejected(signal, rejected, message)
			} else if message != "" {
				c.logger.Warn("otlp export accepted with a server message", "signal", signal, "message", message)
			}
			return nil
		}
		lastErr = err
		v := classify(err)
		if !v.retryable || n >= c.cfg.Retry.Attempts {
			return finalize(ctx, signal, c.endpoint, n+1, lastErr)
		}
		delay := v.wait
		if delay == 0 {
			delay = backoffDelay(c.cfg.Retry.Backoff, n+1)
		}
		if deadline, ok := ctx.Deadline(); (ok && time.Until(deadline) <= delay) || delay > c.cfg.Timeout {
			// waiting would outlive the caller or exceed one attempt's budget;
			// report now and let the bus redeliver at its own pace.
			return finalize(ctx, signal, c.endpoint, n+1, lastErr)
		}
		c.logger.Debug("otlp export failed, retrying", "signal", signal, "attempt", n+1, "delay", delay, "error", err)
		if err := c.sleep(ctx, delay); err != nil {
			return err
		}
	}
}

func (c *Client) once(ctx context.Context, do attempt) (int64, string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	return do(ctx)
}

// rejected records a partial success: a terminal loss for this destination
// by protocol rule.
func (c *Client) rejected(signal string, n int64, message string) {
	c.logger.Warn("otlp export partially rejected; rejected items are not retried (OTLP partial success)",
		"signal", signal, "rejected", n, "message", message)
	if c.metrics != nil {
		c.metrics.OTLPRejectedTotal.WithLabelValues(signal).Add(float64(n))
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// build creates the TLS client configuration from the files in t.
func (t TLSConfig) build() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: t.ServerName}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %s: no certificates found", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load cert_file/key_file: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// grpcTransport is OTLP/gRPC over one lazily connected ClientConn.
type grpcTransport struct {
	conn   *grpc.ClientConn
	traces coltracepb.TraceServiceClient
	logs   collogspb.LogsServiceClient
	md     metadata.MD
}

func newGRPCTransport(cfg Config, tgt target, tlsCfg *tls.Config, userAgent string) (*grpcTransport, error) {
	creds := insecure.NewCredentials()
	if tlsCfg != nil {
		creds = credentials.NewTLS(tlsCfg)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithUserAgent(userAgent),
	}
	if cfg.Compression == CompressionGzip {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(grpcgzip.Name)))
	}
	// NewClient does not connect: the first RPC triggers the connection and a
	// refused endpoint fails that RPC with Unavailable (retryable).
	conn, err := grpc.NewClient(tgt.hostPort, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc client: %w", err)
	}
	md := metadata.New(nil)
	for k, v := range cfg.Headers {
		md.Append(k, v.Reveal())
	}
	return &grpcTransport{
		conn:   conn,
		traces: coltracepb.NewTraceServiceClient(conn),
		logs:   collogspb.NewLogsServiceClient(conn),
		md:     md,
	}, nil
}

func (t *grpcTransport) ctx(ctx context.Context) context.Context {
	if t.md.Len() == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, t.md)
}

func (t *grpcTransport) exportSpans(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	return t.traces.Export(t.ctx(ctx), req)
}

func (t *grpcTransport) exportLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	return t.logs.Export(t.ctx(ctx), req)
}

func (t *grpcTransport) close() error { return t.conn.Close() }

// httpTransport is OTLP/HTTP with protobuf payloads.
type httpTransport struct {
	client    *http.Client
	tracesURL string
	logsURL   string
	headers   map[string]string
	gzip      bool
	userAgent string
}

func newHTTPTransport(cfg Config, tgt target, tlsCfg *tls.Config, userAgent string) *httpTransport {
	scheme := "http"
	if tgt.tls {
		scheme = "https"
	}
	base := scheme + "://" + tgt.hostPort + tgt.basePath
	headers := make(map[string]string, len(cfg.Headers))
	for k, v := range cfg.Headers {
		headers[k] = v.Reveal()
	}
	rt, ok := http.DefaultTransport.(*http.Transport)
	if ok {
		rt = rt.Clone()
	} else {
		rt = &http.Transport{}
	}
	rt.TLSClientConfig = tlsCfg
	return &httpTransport{
		client:    &http.Client{Transport: rt},
		tracesURL: base + tracesPath,
		logsURL:   base + logsPath,
		headers:   headers,
		gzip:      cfg.Compression == CompressionGzip,
		userAgent: userAgent,
	}
}

func (t *httpTransport) exportSpans(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	resp := &coltracepb.ExportTraceServiceResponse{}
	if err := t.post(ctx, t.tracesURL, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *httpTransport) exportLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	resp := &collogspb.ExportLogsServiceResponse{}
	if err := t.post(ctx, t.logsURL, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *httpTransport) close() error {
	t.client.CloseIdleConnections()
	return nil
}

// post sends req as protobuf and decodes a 2xx protobuf body into resp.
func (t *httpTransport) post(ctx context.Context, url string, req, resp proto.Message) error {
	body, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	if t.gzip {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			return fmt.Errorf("gzip request: %w", err)
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("gzip request: %w", err)
		}
		body = buf.Bytes()
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", contentTypeProtobuf)
	httpReq.Header.Set("User-Agent", t.userAgent)
	if t.gzip {
		httpReq.Header.Set("Content-Encoding", "gzip")
	}
	for k, v := range t.headers {
		httpReq.Header.Set(k, v)
	}
	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close() //nolint:errcheck // read-only body
	data, readErr := io.ReadAll(io.LimitReader(httpResp.Body, maxResponseBytes))

	if httpResp.StatusCode < 200 || httpResp.StatusCode > 299 {
		msg := errorMessage(httpResp.Header.Get("Content-Type"), data)
		if readErr != nil {
			msg = strings.TrimSpace(msg + "; response body: " + readErr.Error())
		}
		return &HTTPError{
			StatusCode: httpResp.StatusCode,
			Message:    msg,
			RetryAfter: parseRetryAfter(httpResp.Header.Get("Retry-After"), time.Now()),
		}
	}
	if readErr != nil {
		// The status line already said 2xx: the destination accepted the
		// batch. Only the PartialSuccess details were lost, not the
		// acceptance, so this is not retried as a failure - a retry would
		// duplicate data the destination already has.
		return nil
	}
	if len(data) > 0 && isProtobuf(httpResp.Header.Get("Content-Type")) {
		if err := proto.Unmarshal(data, resp); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

// errorMessage extracts the message of a failed response: the google.rpc.Status
// message when the body is protobuf, the trimmed body text otherwise.
func errorMessage(contentType string, data []byte) string {
	if len(data) == 0 {
		return ""
	}
	if isProtobuf(contentType) {
		var st rpcstatus.Status
		if err := proto.Unmarshal(data, &st); err == nil && st.GetMessage() != "" {
			return truncate(st.GetMessage())
		}
		return ""
	}
	return truncate(strings.TrimSpace(string(data)))
}

func isProtobuf(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	return err == nil && mt == contentTypeProtobuf
}

func truncate(s string) string {
	if len(s) <= maxErrorMessage {
		return s
	}
	return s[:maxErrorMessage] + "…"
}
