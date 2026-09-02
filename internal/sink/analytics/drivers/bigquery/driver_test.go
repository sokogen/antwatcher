package bigquery_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
	"github.com/sokogen/antwatcher/internal/sink"
	"github.com/sokogen/antwatcher/internal/sink/analytics"
	bqdriver "github.com/sokogen/antwatcher/internal/sink/analytics/drivers/bigquery"
)

func TestDriver_RegisteredInDefaultRegistry(t *testing.T) {
	assert.Contains(t, sink.Drivers(sink.ClassAnalytics), bqdriver.Name)
	d, ok := sink.Describers()[config.SinkKey("analytics", "bigquery")]
	require.True(t, ok)

	typed, err := d.Describe(block(t, "project: my-proj\ndataset: github\ntable: actions\ncredentials_file: /etc/sa.json\nensure_table: false"))
	require.NoError(t, err)
	assert.Equal(t, bqdriver.Config{
		Project: "my-proj", Dataset: "github", Table: "actions",
		CredentialsFile: "/etc/sa.json", EnsureTable: false, EnsureView: true,
	}, typed)

	typed, err = d.Describe(yaml.Node{})
	require.NoError(t, err)
	assert.Equal(t, bqdriver.DefaultConfig(), typed, "absent block yields defaults")
	assert.True(t, bqdriver.DefaultConfig().EnsureTable)
	assert.True(t, bqdriver.DefaultConfig().EnsureView)

	_, err = d.Describe(block(t, "project: p\nlocation: EU"))
	require.Error(t, err, "unknown keys are rejected")
}

func TestConfig_Validate(t *testing.T) {
	require.NoError(t, validConfig().Validate())
	require.NoError(t, validConfig().ValidateWith(config.Router{}))

	ok := []bqdriver.Config{
		{Project: "example.com:my-proj", Dataset: "d_1", Table: "actions"},
		{Project: "my-proj-12345", Dataset: "github", Table: "gh actions-2026"},
		{Project: "my-proj", Dataset: "github", Table: "événements"},
	}
	for _, c := range ok {
		require.NoError(t, c.Validate(), "%+v", c)
	}

	bad := []struct {
		name string
		mut  func(*bqdriver.Config)
		want string
	}{
		{"empty project", func(c *bqdriver.Config) { c.Project = "" }, "project is required"},
		{"uppercase project", func(c *bqdriver.Config) { c.Project = "MyProj" }, "not a valid project id"},
		{"short project", func(c *bqdriver.Config) { c.Project = "abc" }, "not a valid project id"},
		{"empty dataset", func(c *bqdriver.Config) { c.Dataset = "" }, "dataset is required"},
		{"dashed dataset", func(c *bqdriver.Config) { c.Dataset = "git-hub" }, "only letters, digits, and underscores"},
		{"empty table", func(c *bqdriver.Config) { c.Table = "" }, "table is required"},
		{"dotted table", func(c *bqdriver.Config) { c.Table = "a.b" }, "may contain only"},
		{"backquoted table", func(c *bqdriver.Config) { c.Table = "a`b" }, "may contain only"},
		{"view suffix", func(c *bqdriver.Config) { c.Table = "actions_current" }, `must not end with "_current"`},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mut(&c)
			err := c.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestConfig_IDs(t *testing.T) {
	c := validConfig()
	assert.Equal(t, "my-proj.github.actions", c.TableID())
	assert.Equal(t, "my-proj.github.actions_current", c.ViewID())
}

func TestFactory_ConfigErrors(t *testing.T) {
	cases := map[string]string{
		"unknown key":  "project: my-proj\ndataset: d\ntable: t\nregion: EU",
		"no project":   "dataset: d\ntable: t",
		"bad dataset":  "project: my-proj\ndataset: my-dataset\ntable: t",
		"wrong type":   "project: my-proj\ndataset: d\ntable: t\nensure_table: maybe",
		"not a map":    "- my-proj",
		"missing file": "project: my-proj\ndataset: d\ntable: t\ncredentials_file: " + filepath.Join(t.TempDir(), "absent.json"),
	}
	for name, text := range cases {
		t.Run(name, func(t *testing.T) {
			s, err := bqdriver.Factory(context.Background(), "bq", block(t, text), sink.Deps{})
			require.Error(t, err)
			assert.Nil(t, s)
		})
	}
}

func TestFactory_BuildsWithoutNetwork(t *testing.T) {
	text := "project: my-proj\ndataset: github\ntable: actions\ncredentials_file: " + serviceAccountFile(t)
	start := time.Now()
	s, err := bqdriver.Factory(context.Background(), "bq", block(t, text), sink.Deps{})
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 5*time.Second, "construction must not wait on the network")
	assert.Equal(t, "bq", s.Name())
	assert.Equal(t, sink.ClassAnalytics, s.Class())
	assert.IsType(t, &analytics.Sink{}, s)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close(), "closing twice is harmless")
}

func TestNew_ValidatesAndClosesCleanly(t *testing.T) {
	_, err := bqdriver.New(bqdriver.Config{}, bqdriver.Options{})
	require.Error(t, err)

	cfg := validConfig()
	cfg.CredentialsFile = serviceAccountFile(t)
	w, err := bqdriver.New(cfg, bqdriver.Options{ClientOptions: []option.ClientOption{option.WithEndpoint("http://127.0.0.1:1/")}})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// a write after Close fails permanently instead of dialling
	err = w.Write(context.Background(), fixtureRecords(t, "workflow_run.completed"))
	require.Error(t, err)
	assert.True(t, sink.IsPermanent(err))
}

// unroutable returns a loopback address nothing listens on.
func unroutable(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

// The real adapters are driven against a loopback endpoint that refuses
// connections: the stream opens offline, the append fails at the transport
// and comes back retryable, and the metadata call fails the same way. This
// is the "destination unreachable" path the sink degrades on.
func TestRealClients_UnreachableEndpointIsRetryable(t *testing.T) {
	addr := unroutable(t)
	cfg := validConfig()
	recs := fixtureRecords(t, "workflow_run.completed")

	t.Run("write", func(t *testing.T) {
		w, err := bqdriver.New(cfg, bqdriver.Options{ClientOptions: []option.ClientOption{
			option.WithEndpoint(addr), option.WithoutAuthentication(),
		}})
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = w.Write(ctx, recs)
		require.Error(t, err)
		assert.False(t, sink.IsPermanent(err), err)
		assert.Contains(t, err.Error(), "my-proj.github.actions")
	})
	t.Run("ensure schema", func(t *testing.T) {
		w, err := bqdriver.New(cfg, bqdriver.Options{ClientOptions: []option.ClientOption{
			option.WithEndpoint("http://" + addr + "/"), option.WithoutAuthentication(),
		}})
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = w.EnsureSchema(ctx, analytics.Current)
		require.Error(t, err)
		assert.False(t, sink.IsPermanent(err), err)
		assert.Contains(t, err.Error(), "metadata:")
	})
}
