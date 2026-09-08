package sink

import (
	"context"
	"errors"
	"log/slog"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

// levelTrace is below slog.LevelDebug: Watermill's trace lines are emitted
// only when a handler is configured that verbose.
const levelTrace = slog.LevelDebug - 4

// handlerErrorMessage is the line the Watermill router logs at error level
// for every handler error. The sink handler has already logged the failure
// with its classification, so the adapter demotes this line to debug unless
// the error is a recovered panic, which nothing else reports.
const handlerErrorMessage = "Handler returned error"

// wmLogger adapts slog to watermill.LoggerAdapter.
type wmLogger struct {
	l *slog.Logger
}

func newWatermillLogger(l *slog.Logger) watermill.LoggerAdapter {
	return wmLogger{l: l.With("component", "watermill")}
}

// Error implements watermill.LoggerAdapter.
func (w wmLogger) Error(msg string, err error, fields watermill.LogFields) {
	level := slog.LevelError
	var panicked middleware.RecoveredPanicError
	if msg == handlerErrorMessage && !errors.As(err, &panicked) {
		level = slog.LevelDebug
	}
	w.log(level, msg, append(attrs(fields), slog.Any("err", err))...)
}

// Info implements watermill.LoggerAdapter.
func (w wmLogger) Info(msg string, fields watermill.LogFields) {
	w.log(slog.LevelInfo, msg, attrs(fields)...)
}

// Debug implements watermill.LoggerAdapter.
func (w wmLogger) Debug(msg string, fields watermill.LogFields) {
	w.log(slog.LevelDebug, msg, attrs(fields)...)
}

// Trace implements watermill.LoggerAdapter at a level below debug.
func (w wmLogger) Trace(msg string, fields watermill.LogFields) {
	w.log(levelTrace, msg, attrs(fields)...)
}

// With implements watermill.LoggerAdapter.
func (w wmLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	args := make([]any, 0, len(fields))
	for _, a := range attrs(fields) {
		args = append(args, a)
	}
	return wmLogger{l: w.l.With(args...)}
}

func (w wmLogger) log(level slog.Level, msg string, attrs ...slog.Attr) {
	w.l.LogAttrs(context.Background(), level, msg, attrs...)
}

func attrs(fields watermill.LogFields) []slog.Attr {
	out := make([]slog.Attr, 0, len(fields))
	for k, v := range fields {
		out = append(out, slog.Any(k, v))
	}
	return out
}
