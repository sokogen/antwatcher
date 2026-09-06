// Package receiver is the webhook ingress of antwatcher: it verifies GitHub's
// HMAC signature over the raw body, wraps the delivery in an event.Envelope,
// and publishes it on the bus. GitHub receives a 2xx only after Bus.Publish
// returned nil, so on a bus with DurablePublish an acknowledged delivery is
// stored; any publish failure or timeout answers 503 and GitHub records a
// failed delivery for the recovery loop to pick up.
//
// The handler mounts only the webhook path and GET /healthz for load
// balancers. Readiness, metrics, and status live on the admin listener, and
// readiness depends on the bus alone: sinks being down never fails ingress.
package receiver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/metrics"
)

// Headers and values of the GitHub webhook protocol handled here.
const (
	// HeaderSignature carries "sha256=<hex HMAC-SHA256 of the raw body>".
	HeaderSignature = "X-Hub-Signature-256"
	signaturePrefix = "sha256="
	// EventPing is answered 200 without publishing; GitHub sends it when the
	// webhook is created or tested.
	EventPing = "ping"
)

// unknownEvent is the event label recorded for requests rejected before the
// signature was verified. The header is attacker-controlled until then and
// must not create metric series.
const unknownEvent = "unknown"

// Handler is the webhook HTTP handler. Use New.
type Handler struct {
	cfg     config.Server
	bus     bus.Bus
	metrics *metrics.Metrics
	logger  *slog.Logger
	now     func() time.Time
}

// New builds the webhook handler mounting POST cfg.WebhookPath and GET
// /healthz. Every other path is 404 and every other method on the webhook
// path is 405. The topic is the one the bus carries. logger may be nil.
func New(cfg config.Server, b bus.Bus, m *metrics.Metrics, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	h := &Handler{cfg: cfg, bus: b, metrics: m, logger: logger, now: time.Now}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+cfg.WebhookPath, h.serveWebhook)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	return mux
}

// serveWebhook runs the delivery flow in order: read (bounded), verify HMAC,
// ping, envelope, publish, answer. Every exit path records one
// antwatcher_webhooks_total sample.
func (h *Handler) serveWebhook(w http.ResponseWriter, r *http.Request) {
	start := h.now()
	h.metrics.WebhooksInflight.Inc()
	defer h.metrics.WebhooksInflight.Dec()

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.cfg.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.count(unknownEvent, metrics.WebhookBadRequest)
			h.logger.Warn("webhook rejected: body too large", "limit", h.cfg.MaxBodyBytes, "remote", r.RemoteAddr)
			writeError(w, http.StatusRequestEntityTooLarge, "body exceeds max_body_bytes", "")
			return
		}
		h.count(unknownEvent, metrics.WebhookBadRequest)
		h.logger.Warn("webhook rejected: read body", "err", err, "remote", r.RemoteAddr)
		writeError(w, http.StatusBadRequest, "read body", "")
		return
	}

	if !verifySignature(h.cfg.WebhookSecret.Reveal(), r.Header.Get(HeaderSignature), body) {
		h.count(unknownEvent, metrics.WebhookRejectedSignature)
		h.logger.Warn("webhook rejected: signature", "remote", r.RemoteAddr,
			"delivery", r.Header.Get(event.HeaderDelivery), "signature_present", r.Header.Get(HeaderSignature) != "")
		writeError(w, http.StatusUnauthorized, "invalid or missing "+HeaderSignature, "")
		return
	}

	// From here the request is authenticated: header values are GitHub's.
	guid := strings.TrimSpace(r.Header.Get(event.HeaderDelivery))
	eventName := strings.TrimSpace(r.Header.Get(event.HeaderEvent))
	if eventName == EventPing {
		h.count(EventPing, metrics.WebhookPing)
		h.logger.Info("webhook ping", "delivery", guid, "hook_id", r.Header.Get(event.HeaderHookID))
		writeJSON(w, http.StatusOK, map[string]any{"delivery": guid, "ping": true})
		return
	}

	env, err := event.FromWebhook(r.Header, body, start)
	if err != nil {
		h.count(labelEvent(eventName), metrics.WebhookBadRequest)
		h.logger.Warn("webhook rejected: bad request", "err", err, "delivery", guid, "event", eventName)
		writeError(w, http.StatusBadRequest, err.Error(), guid)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.PublishTimeout)
	defer cancel()
	publishStart := h.now()
	err = h.bus.Publish(ctx, event.ToMessage(env))
	publishLatency := h.now().Sub(publishStart)
	h.metrics.WebhookPublishSeconds.Observe(publishLatency.Seconds())
	if err != nil {
		result := metrics.WebhookPublishFailed
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			result = metrics.WebhookPublishTimeout
		}
		h.count(env.Event, result)
		h.logger.Error("webhook publish failed", "err", err, "result", result,
			"delivery", env.DeliveryGUID, "event", env.Event, "action", env.Action,
			"repository", env.Repository, "publish_ms", publishLatency.Milliseconds())
		// The driver's error text (broker addresses, stream names) stays in the
		// log above: this body lands in GitHub's delivery log, which is a wider
		// audience than the operator. The GUID is the correlation handle.
		writeError(w, http.StatusServiceUnavailable, "publish failed", env.DeliveryGUID)
		return
	}

	// Publish returned nil: the delivery is as durable as the bus declares.
	// Only now may GitHub see a 2xx.
	h.count(env.Event, metrics.WebhookPublished)
	h.logger.Info("webhook published",
		"delivery", env.DeliveryGUID, "event", env.Event, "action", env.Action,
		"repository", env.Repository, "latency_ms", h.now().Sub(start).Milliseconds(),
		"publish_ms", publishLatency.Milliseconds())
	writeJSON(w, http.StatusOK, map[string]any{"delivery": env.DeliveryGUID})
}

func (h *Handler) count(eventName, result string) {
	h.metrics.WebhooksTotal.WithLabelValues(eventName, result).Inc()
}

// labelEvent maps an authenticated event header to a label value.
func labelEvent(eventName string) string {
	if eventName == "" {
		return unknownEvent
	}
	return eventName
}

// verifySignature checks header against HMAC-SHA256(secret, body). The
// comparison is constant time; a missing or malformed header fails.
func verifySignature(secret, header string, body []byte) bool {
	if secret == "" || !strings.HasPrefix(header, signaturePrefix) {
		return false
	}
	got, err := hex.DecodeString(strings.TrimPrefix(header, signaturePrefix))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hmac.Equal(got, mac.Sum(nil))
}

// Sign returns the X-Hub-Signature-256 value GitHub would send for body.
// Exported for tests of downstream packages that drive the receiver.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError answers with {"error": msg, "delivery": guid}; delivery is
// omitted when unknown. GitHub shows the body in the delivery log, so the
// GUID lets an operator match it with the recovery loop.
func writeError(w http.ResponseWriter, code int, msg, guid string) {
	body := map[string]any{"error": msg}
	if guid != "" {
		body["delivery"] = guid
	}
	writeJSON(w, code, body)
}
