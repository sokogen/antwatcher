package gochannel_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/bus"
	"github.com/sokogen/antwatcher/internal/bus/bustest"
	"github.com/sokogen/antwatcher/internal/bus/gochannel"
	"github.com/sokogen/antwatcher/internal/config"
)

const topic = "antwatcher.events"

func TestConformance(t *testing.T) {
	bustest.Run(t, func(_ *testing.T) bus.Bus { return gochannel.New(topic, nil) })
}

func TestCapabilities(t *testing.T) {
	b := gochannel.New(topic, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	assert.Equal(t, bus.Capabilities{FanOut: true, HistoricalReplay: true, Retention: bus.RetentionNone}, b.Capabilities())
	assert.Equal(t, topic, b.Topic())

	// The policy consequences of these capabilities, stated once here.
	_, err := bus.ResolveIngress(b.Capabilities(), bus.ModeFail)
	require.ErrorIs(t, err, bus.ErrMissingCapability, "gochannel is refused as ingress in fail mode")
	warnings, err := bus.ResolveIngress(b.Capabilities(), bus.ModeDegrade)
	require.NoError(t, err)
	assert.Len(t, warnings, 1)
	effective, warnings, err := bus.ResolveConsumer(b.Capabilities(), bus.Earliest, bus.ModeFail)
	require.NoError(t, err, "Earliest is served in fail mode because history is replayed")
	assert.Equal(t, bus.Earliest, effective)
	assert.Len(t, warnings, 2, "positions lost on restart, no dedup")
}

func TestRegisteredInDefaultRegistry(t *testing.T) {
	assert.Contains(t, bus.Names(), gochannel.Name)

	var cfg config.Bus
	require.NoError(t, yaml.Unmarshal([]byte("driver: gochannel\ntopic: t\ngochannel: {}\n"), &cfg))
	b, err := bus.Open(context.Background(), cfg, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "t", b.Topic())
	require.NoError(t, b.Close())

	require.NoError(t, yaml.Unmarshal([]byte("driver: gochannel\ntopic: t\ngochannel:\n  buffer: 10\n"), &cfg))
	_, err = bus.Open(context.Background(), cfg, nil, nil)
	require.Error(t, err, "unknown driver keys are rejected")
	assert.Contains(t, err.Error(), "buffer")
}

func TestDescribe(t *testing.T) {
	v, err := gochannel.Describe(yaml.Node{})
	require.NoError(t, err)
	assert.Equal(t, gochannel.Config{}, v)

	var node yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte("persistent: false\n"), &node))
	_, err = gochannel.Describe(*node.Content[0])
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persistent")

	describers := config.Describers{Bus: bus.Describers()}
	cfg := config.Default()
	cfg.Bus.Driver = gochannel.Name
	require.NoError(t, config.ValidateDrivers(cfg, describers))
}

func TestOpenRequiresTopic(t *testing.T) {
	_, err := gochannel.Open(context.Background(), yaml.Node{}, "", nil, nil)
	require.Error(t, err)
}

func TestNowSkipsEarlierPublishes(t *testing.T) {
	b := gochannel.New(topic, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	before1, before2 := newMessage("before-1"), newMessage("before-2")
	require.NoError(t, b.Publish(context.Background(), before1))
	require.NoError(t, b.Publish(context.Background(), before2))

	ch := subscribe(t, b, "now", bus.Now)
	after := newMessage("after")
	require.NoError(t, b.Publish(context.Background(), after))

	got := receive(t, ch)
	assert.Equal(t, "after", got.UUID)
	assert.Equal(t, message.Metadata{"k": "v"}, got.Metadata, "the sequence tag is stripped before delivery")
	got.Ack()
	expectQuiet(t, ch)
}

func TestEarliestReplaysEverything(t *testing.T) {
	b := gochannel.New(topic, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	require.NoError(t, b.Publish(context.Background(), newMessage("one")))
	require.NoError(t, b.Publish(context.Background(), newMessage("two")))

	ch := subscribe(t, b, "earliest", bus.Earliest)
	require.NoError(t, b.Publish(context.Background(), newMessage("three")))

	got := map[string]bool{}
	for range 3 {
		msg := receive(t, ch)
		got[msg.UUID] = true
		msg.Ack()
	}
	assert.Equal(t, map[string]bool{"one": true, "two": true, "three": true}, got)
}

func TestPublishDoesNotModifyCallerMessage(t *testing.T) {
	b := gochannel.New(topic, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	msg := newMessage("m")
	require.NoError(t, b.Publish(context.Background(), msg))
	assert.Equal(t, message.Metadata{"k": "v"}, msg.Metadata)
}

func TestPublishErrors(t *testing.T) {
	b := gochannel.New(topic, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	require.Error(t, b.Publish(context.Background(), nil))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, b.Publish(ctx, newMessage("m")), context.Canceled)
}

func TestSubscribeErrors(t *testing.T) {
	b := gochannel.New(topic, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	_, err := b.Subscribe(context.Background(), "zero", bus.SubscribeOptions{})
	require.Error(t, err, "the zero start position is invalid")
	assert.Contains(t, err.Error(), "invalid start position")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = b.Subscribe(ctx, "cancelled", bus.SubscribeOptions{StartFrom: bus.Now})
	require.ErrorIs(t, err, context.Canceled)
}

func TestSubscriberCloseEndsChannel(t *testing.T) {
	b := gochannel.New(topic, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	sub, err := b.Subscribe(context.Background(), "c", bus.SubscribeOptions{StartFrom: bus.Now})
	require.NoError(t, err)
	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	require.NoError(t, sub.Close())
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel closes after Close")
	case <-time.After(5 * time.Second):
		t.Fatal("channel not closed after subscriber Close")
	}
	require.NoError(t, sub.Close(), "Close is idempotent")
	_, err = sub.Subscribe(context.Background(), topic)
	require.Error(t, err, "a closed subscriber cannot subscribe again")
}

func TestSubscriptionEndsWithContext(t *testing.T) {
	b := gochannel.New(topic, nil)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	sub, err := b.Subscribe(context.Background(), "c", bus.SubscribeOptions{StartFrom: bus.Now})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sub.Close()) })
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)
	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok)
	case <-time.After(5 * time.Second):
		t.Fatal("channel not closed after ctx cancel")
	}
}

func TestBusCloseEndsSubscriptions(t *testing.T) {
	b := gochannel.New(topic, nil)
	ch := subscribe(t, b, "c", bus.Now)
	require.NoError(t, b.Close())
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "bus Close ends every subscription")
	case <-time.After(5 * time.Second):
		t.Fatal("channel not closed after bus Close")
	}
}

func newMessage(uuid string) *message.Message {
	msg := message.NewMessage(uuid, []byte(`{}`))
	msg.Metadata.Set("k", "v")
	return msg
}

func subscribe(t *testing.T, b bus.Bus, name string, from bus.StartPosition) <-chan *message.Message {
	t.Helper()
	sub, err := b.Subscribe(context.Background(), name, bus.SubscribeOptions{StartFrom: from})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sub.Close()) })
	ch, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)
	return ch
}

func receive(t *testing.T, ch <-chan *message.Message) *message.Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		require.True(t, ok, "channel closed")
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("no message")
		return nil
	}
}

func expectQuiet(t *testing.T, ch <-chan *message.Message) {
	t.Helper()
	select {
	case msg := <-ch:
		t.Fatalf("unexpected message %s", msg.UUID)
	case <-time.After(300 * time.Millisecond):
	}
}
