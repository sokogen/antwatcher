package bigquery_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/types/descriptorpb"
	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/event"
	"github.com/sokogen/antwatcher/internal/model"
	"github.com/sokogen/antwatcher/internal/sink/analytics"
	bqdriver "github.com/sokogen/antwatcher/internal/sink/analytics/drivers/bigquery"
)

var received = time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)

func block(t *testing.T, text string) yaml.Node {
	t.Helper()
	var n yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte(text), &n))
	if n.Kind == yaml.DocumentNode && len(n.Content) == 1 {
		return *n.Content[0]
	}
	return n
}

func validConfig() bqdriver.Config {
	return bqdriver.Config{Project: "my-proj", Dataset: "github", Table: "actions", EnsureTable: true, EnsureView: true}
}

func fixtureEnvelope(t *testing.T, fixture string) event.Envelope {
	t.Helper()
	name, _, _ := strings.Cut(fixture, ".")
	hdr := http.Header{}
	hdr.Set(event.HeaderDelivery, "guid-"+fixture)
	hdr.Set(event.HeaderEvent, name)
	env, err := event.FromWebhook(hdr, event.LoadFixture(t, fixture), received)
	require.NoError(t, err)
	return env
}

// fixtureRecords projects a fixture into analytics records.
func fixtureRecords(t *testing.T, fixture string) []analytics.Record {
	t.Helper()
	exec, err := model.Normalize(fixtureEnvelope(t, fixture))
	require.NoError(t, err)
	recs, err := analytics.Project(exec)
	require.NoError(t, err)
	return recs
}

// serviceAccountFile writes a syntactically valid service account JSON with
// a throwaway key, so the clients can be constructed without credentials
// on the machine. Its token endpoint is unroutable: any request would fail,
// which is how the tests prove nothing is sent.
func serviceAccountFile(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	sa := map[string]string{
		"type":           "service_account",
		"project_id":     "my-proj",
		"private_key_id": "test",
		"private_key":    string(pemKey),
		"client_email":   "antwatcher@my-proj.iam.gserviceaccount.com",
		"client_id":      "1",
		"token_uri":      "http://127.0.0.1:1/token",
	}
	data, err := json.Marshal(sa)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "sa.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func apiError(code int) error {
	return &googleapi.Error{Code: code, Message: http.StatusText(code)}
}

// fakeTables is an in-memory Tables: tables that exist have metadata,
// missing ones answer 404, and every call is recorded.
type fakeTables struct {
	mu       sync.Mutex
	existing map[string]*bq.TableMetadata
	created  map[string]*bq.TableMetadata
	// metadataErr / createErr override the answer per table id.
	metadataErr map[string]error
	createErr   map[string]error
	calls       []string
}

func newFakeTables() *fakeTables {
	return &fakeTables{
		existing:    map[string]*bq.TableMetadata{},
		created:     map[string]*bq.TableMetadata{},
		metadataErr: map[string]error{},
		createErr:   map[string]error{},
	}
}

func (f *fakeTables) Table(id string) bqdriver.Table { return &fakeTable{f: f, id: id} }

func (f *fakeTables) record(call string) {
	f.calls = append(f.calls, call)
}

func (f *fakeTables) callList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

type fakeTable struct {
	f  *fakeTables
	id string
}

func (t *fakeTable) Metadata(context.Context) (*bq.TableMetadata, error) {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.f.record("metadata " + t.id)
	if err := t.f.metadataErr[t.id]; err != nil {
		return nil, err
	}
	if md, ok := t.f.existing[t.id]; ok {
		return md, nil
	}
	return nil, apiError(http.StatusNotFound)
}

func (t *fakeTable) Create(_ context.Context, md *bq.TableMetadata) error {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.f.record("create " + t.id)
	if err := t.f.createErr[t.id]; err != nil {
		return err
	}
	t.f.created[t.id] = md
	t.f.existing[t.id] = md
	return nil
}

// fakeAppender records appended batches and answers with a programmable
// error per call.
type fakeAppender struct {
	mu      sync.Mutex
	batches [][][]byte
	fn      func(call int) error
	closed  int
}

func (a *fakeAppender) Append(_ context.Context, rows [][]byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	call := len(a.batches)
	a.batches = append(a.batches, rows)
	if a.fn != nil {
		return a.fn(call)
	}
	return nil
}

func (a *fakeAppender) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed++
	return nil
}

func (a *fakeAppender) batchList() [][][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([][][]byte(nil), a.batches...)
}

// fakeOpener hands out app and records how often it was asked and with
// which descriptor; err makes it fail instead.
type fakeOpener struct {
	mu    sync.Mutex
	app   *fakeAppender
	err   error
	opens int
	descs []*descriptorpb.DescriptorProto
	block chan struct{} // when set, open waits for this or ctx before returning
}

func (o *fakeOpener) open(ctx context.Context, desc *descriptorpb.DescriptorProto) (bqdriver.Appender, error) {
	o.mu.Lock()
	o.opens++
	o.descs = append(o.descs, desc)
	err, block, app := o.err, o.block, o.app
	o.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	return app, nil
}

func (o *fakeOpener) openCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.opens
}
