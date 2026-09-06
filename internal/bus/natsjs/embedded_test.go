package natsjs_test

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/bus/natsjs"
)

func TestStartEmbedded(t *testing.T) {
	_, err := natsjs.StartEmbedded(natsjs.Config{}, nil)
	require.Error(t, err, "store_dir is required")

	dir := filepath.Join(t.TempDir(), "nested", "store")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	srv, err := natsjs.StartEmbedded(natsjs.Config{StoreDir: dir}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { natsjs.StopEmbedded(srv) })

	assert.DirExists(t, dir, "store_dir is created")
	assert.True(t, srv.JetStreamEnabled())
	assert.Nil(t, srv.Addr(), "no TCP listener")

	nc, err := nats.Connect("", nats.InProcessServer(srv))
	require.NoError(t, err, "clients connect in process")
	nc.Close()
	assert.Contains(t, logs.String(), "component=nats-server", "server log lines are routed to slog")
}

func TestStartEmbedded_RefusesASecondServerOnTheSameStoreDir(t *testing.T) {
	dir := t.TempDir()
	srv, err := natsjs.StartEmbedded(natsjs.Config{StoreDir: dir}, nil)
	require.NoError(t, err)

	// Two servers over one JetStream file store corrupt each other; a forward
	// sink inheriting the ingress defaults is the way to get there by accident.
	_, err = natsjs.StartEmbedded(natsjs.Config{StoreDir: dir}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already running on store_dir")
	_, err = natsjs.StartEmbedded(natsjs.Config{StoreDir: filepath.Join(dir, "..", filepath.Base(dir))}, nil)
	require.Error(t, err, "the same directory reached by another path")

	natsjs.StopEmbedded(srv)
	srv, err = natsjs.StartEmbedded(natsjs.Config{StoreDir: dir}, nil)
	require.NoError(t, err, "the directory is free again once the server stops")
	natsjs.StopEmbedded(srv)
	natsjs.StopEmbedded(srv) // no-op on an already stopped server
	natsjs.StopEmbedded(nil)
}

func TestTestServerStopStart(t *testing.T) {
	ts := natsjs.NewTestServer(t)
	assert.True(t, ts.Running())
	assert.DirExists(t, ts.StoreDir())

	conn, err := ts.InProcessConn()
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	ts.Stop()
	assert.False(t, ts.Running())
	_, err = ts.InProcessConn()
	require.ErrorIs(t, err, natsjs.ErrServerStopped)
	ts.Stop() // no-op while stopped

	ts.Start()
	assert.True(t, ts.Running())
	ts.Start() // no-op while running
	conn, err = ts.InProcessConn()
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	cfg := ts.Config()
	require.NoError(t, cfg.Validate())
	assert.Equal(t, ts.StoreDir(), cfg.StoreDir)
	assert.Less(t, cfg.NakDelayMax, time.Second, "short nak delays keep redelivery tests fast")
}
