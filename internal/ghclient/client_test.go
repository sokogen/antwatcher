package ghclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/config"
)

// stub is a minimal GitHub REST API mounted under /api/v3/ that records every
// request and serves canned deliveries and hooks.
type stub struct {
	srv *httptest.Server
	mux *http.ServeMux

	mu       sync.Mutex
	requests []*http.Request
}

func newStub(t *testing.T) *stub {
	t.Helper()
	s := &stub{mux: http.NewServeMux()}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests = append(s.requests, r.Clone(context.Background()))
		s.mu.Unlock()
		s.mux.ServeHTTP(w, r)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *stub) client(t *testing.T) *Client {
	t.Helper()
	c, err := New(Auth{Type: config.AuthTypeToken, Token: "tok-secret"}, Options{APIBaseURL: s.srv.URL, Timeout: 5 * time.Second})
	require.NoError(t, err)
	return c
}

func (s *stub) recorded() []*http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*http.Request(nil), s.requests...)
}

func (s *stub) paths() []string {
	var out []string
	for _, r := range s.recorded() {
		out = append(out, r.Method+" "+r.URL.RequestURI())
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// deliveryJSON mirrors the wire shape of a hook delivery.
type deliveryJSON struct {
	ID          int64     `json:"id"`
	GUID        string    `json:"guid"`
	DeliveredAt time.Time `json:"delivered_at"`
	Redelivery  bool      `json:"redelivery"`
	StatusCode  int       `json:"status_code"`
	Event       string    `json:"event"`
	Action      string    `json:"action"`
}

// servePages mounts a cursor-paginated deliveries endpoint at path. Page i is
// reachable with cursor "c<i>" (the first page with no cursor) and links to
// the next page while one exists.
func (s *stub) servePages(path string, pages [][]deliveryJSON) {
	s.mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		idx := 0
		if c := q.Get("cursor"); c != "" {
			n, err := strconv.Atoi(c[1:])
			if err != nil || n >= len(pages) {
				writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"message": "bad cursor " + c})
				return
			}
			idx = n
		}
		if idx+1 < len(pages) {
			w.Header().Set("Link", fmt.Sprintf(`<https://api.github.com%s?cursor=c%d&per_page=100>; rel="next"`, path, idx+1))
		}
		writeJSON(w, http.StatusOK, pages[idx])
	})
}

func hookJSON(id int64, url string) map[string]any {
	return map[string]any{"id": id, "config": map[string]any{"url": url, "content_type": "json"}}
}

var base = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

// deliveriesBack builds n deliveries newest first, one minute apart, starting
// at base.
func deliveriesBack(startID int64, n int, code int) []deliveryJSON {
	out := make([]deliveryJSON, 0, n)
	for i := range n {
		out = append(out, deliveryJSON{
			ID:          startID - int64(i),
			GUID:        fmt.Sprintf("guid-%d", startID-int64(i)),
			DeliveredAt: base.Add(-time.Duration(i) * time.Minute),
			StatusCode:  code,
			Event:       "workflow_job",
			Action:      "completed",
		})
	}
	return out
}

func TestNew_AuthTypes(t *testing.T) {
	cases := []struct {
		name string
		auth Auth
		want error
	}{
		{"github_app reserved", Auth{Type: config.AuthTypeGitHubApp, Token: "x"}, ErrUnsupportedAuth},
		{"unknown type", Auth{Type: "basic", Token: "x"}, ErrUnsupportedAuth},
		{"empty type", Auth{Token: "x"}, nil},
		{"empty token", Auth{Type: config.AuthTypeToken}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New(tc.auth, Options{})
			require.Error(t, err)
			assert.Nil(t, c)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
			}
			assert.NotContains(t, err.Error(), "x")
		})
	}
	t.Run("token ok", func(t *testing.T) {
		c, err := New(AuthFromConfig(config.RecoveryAuth{Type: config.AuthTypeToken, Token: "tok"}), Options{})
		require.NoError(t, err)
		assert.Equal(t, "https://api.github.com/", c.gh.BaseURL())
		assert.Equal(t, "antwatcher", c.gh.UserAgent())
	})
	t.Run("enterprise base url", func(t *testing.T) {
		c, err := New(Auth{Type: config.AuthTypeToken, Token: "tok"}, Options{APIBaseURL: "https://ghe.example.com", UserAgent: "antwatcher/1.0"})
		require.NoError(t, err)
		assert.Equal(t, "https://ghe.example.com/api/v3/", c.gh.BaseURL())
		assert.Equal(t, "antwatcher/1.0", c.gh.UserAgent())
	})
	t.Run("invalid base url", func(t *testing.T) {
		_, err := New(Auth{Type: config.AuthTypeToken, Token: "tok"}, Options{APIBaseURL: "://bad"})
		require.Error(t, err)
	})
	t.Run("custom http client", func(t *testing.T) {
		_, err := New(Auth{Type: config.AuthTypeToken, Token: "tok"}, Options{HTTPClient: &http.Client{}})
		require.NoError(t, err)
	})
}

func TestTargetFromConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     config.RecoveryTarget
		want    Target
		wantErr bool
	}{
		{"repo", config.RecoveryTarget{Repo: "acme/widgets", HookID: 7}, Target{Owner: "acme", Repo: "widgets", HookID: 7}, false},
		{"org", config.RecoveryTarget{Org: "acme"}, Target{Org: "acme"}, false},
		{"both", config.RecoveryTarget{Repo: "acme/widgets", Org: "acme"}, Target{}, true},
		{"neither", config.RecoveryTarget{}, Target{}, true},
		{"no slash", config.RecoveryTarget{Repo: "acme"}, Target{}, true},
		{"empty owner", config.RecoveryTarget{Repo: "/widgets"}, Target{}, true},
		{"nested", config.RecoveryTarget{Repo: "acme/a/b"}, Target{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TargetFromConfig(tc.cfg)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTarget_Strings(t *testing.T) {
	assert.Equal(t, "acme/widgets", Target{Owner: "acme", Repo: "widgets"}.String())
	assert.Equal(t, "acme/widgets#7", Target{Owner: "acme", Repo: "widgets", HookID: 7}.String())
	assert.Equal(t, "org:acme", Target{Org: "acme"}.Name())
	assert.Equal(t, "org:acme#9", Target{Org: "acme", HookID: 9}.String())
}

func TestDelivery_Succeeded(t *testing.T) {
	assert.True(t, Delivery{StatusCode: 200}.Succeeded())
	assert.True(t, Delivery{StatusCode: 204}.Succeeded())
	assert.False(t, Delivery{StatusCode: 503}.Succeeded())
	assert.False(t, Delivery{}.Succeeded())
}

func TestListDeliveries_PaginatesUntilSince(t *testing.T) {
	s := newStub(t)
	// 3 pages of 3, newest first: ids 9..1, delivered base, base-1m, ... base-8m.
	all := deliveriesBack(9, 9, 503)
	all[4].StatusCode = 200
	all[4].Redelivery = true
	s.servePages("/api/v3/repos/acme/widgets/hooks/7/deliveries", [][]deliveryJSON{all[0:3], all[3:6], all[6:9]})
	c := s.client(t)
	target := Target{Owner: "acme", Repo: "widgets", HookID: 7}

	// since = base-4m keeps ids 9..5 (5 deliveries) and stops inside page 2.
	got, err := c.ListDeliveries(context.Background(), target, base.Add(-4*time.Minute))
	require.NoError(t, err)
	require.Len(t, got, 5)
	assert.Equal(t, Delivery{ID: 9, GUID: "guid-9", DeliveredAt: base, StatusCode: 503, Event: "workflow_job", Action: "completed"}, got[0])
	assert.Equal(t, Delivery{ID: 5, GUID: "guid-5", DeliveredAt: base.Add(-4 * time.Minute), StatusCode: 200, Redelivery: true, Event: "workflow_job", Action: "completed"}, got[4])
	assert.Equal(t, []string{
		"GET /api/v3/repos/acme/widgets/hooks/7/deliveries?per_page=100",
		"GET /api/v3/repos/acme/widgets/hooks/7/deliveries?cursor=c1&per_page=100",
	}, s.paths(), "stops on the page that crosses since; page 3 is never fetched")
	for _, r := range s.recorded() {
		assert.Equal(t, "Bearer tok-secret", r.Header.Get("Authorization"))
		assert.Equal(t, "antwatcher", r.Header.Get("User-Agent"))
	}

	// since far in the past walks all three pages and returns everything.
	s.mu.Lock()
	s.requests = nil
	s.mu.Unlock()
	got, err = c.ListDeliveries(context.Background(), target, base.Add(-72*time.Hour))
	require.NoError(t, err)
	assert.Len(t, got, 9)
	assert.Len(t, s.paths(), 3)

	// since in the future returns nothing after one page.
	got, err = c.ListDeliveries(context.Background(), target, base.Add(time.Second))
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestListDeliveries_OrgEndpointAndEmpty(t *testing.T) {
	s := newStub(t)
	s.servePages("/api/v3/orgs/acme/hooks/3/deliveries", [][]deliveryJSON{{}})
	c := s.client(t)
	got, err := c.ListDeliveries(context.Background(), Target{Org: "acme", HookID: 3}, base)
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Equal(t, []string{"GET /api/v3/orgs/acme/hooks/3/deliveries?per_page=100"}, s.paths())
}

func TestListDeliveries_RepeatedCursorStops(t *testing.T) {
	s := newStub(t)
	s.mux.HandleFunc("GET /api/v3/repos/acme/widgets/hooks/7/deliveries", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Link", `<https://api.github.com/repos/acme/widgets/hooks/7/deliveries?cursor=same>; rel="next"`)
		writeJSON(w, http.StatusOK, deliveriesBack(1, 1, 503))
	})
	c := s.client(t)
	got, err := c.ListDeliveries(context.Background(), Target{Owner: "acme", Repo: "widgets", HookID: 7}, base.Add(-time.Hour))
	require.NoError(t, err)
	assert.Len(t, got, 2, "first page plus the page fetched with the cursor, then the repeated cursor ends the walk")
	assert.Len(t, s.paths(), 2)
}

func TestListDeliveries_Errors(t *testing.T) {
	s := newStub(t)
	s.mux.HandleFunc("GET /api/v3/repos/acme/widgets/hooks/7/deliveries", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"message": "boom"})
	})
	s.mux.HandleFunc("GET /api/v3/repos/acme/widgets/hooks/8/deliveries", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"message": "Bad credentials"})
	})
	c := s.client(t)

	t.Run("unknown hook id", func(t *testing.T) {
		_, err := c.ListDeliveries(context.Background(), Target{Owner: "acme", Repo: "widgets"}, base)
		require.ErrorContains(t, err, "hook id is unknown")
		assert.Empty(t, s.paths())
	})
	t.Run("server error is not rate limited", func(t *testing.T) {
		_, err := c.ListDeliveries(context.Background(), Target{Owner: "acme", Repo: "widgets", HookID: 7}, base)
		require.ErrorContains(t, err, "boom")
		assert.False(t, IsRateLimited(err))
	})
	t.Run("bad credentials", func(t *testing.T) {
		_, err := c.ListDeliveries(context.Background(), Target{Owner: "acme", Repo: "widgets", HookID: 8}, base)
		require.ErrorContains(t, err, "Bad credentials")
		assert.NotContains(t, err.Error(), "tok-secret")
	})
	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := c.ListDeliveries(ctx, Target{Owner: "acme", Repo: "widgets", HookID: 7}, base)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestListDeliveries_RateLimited(t *testing.T) {
	t.Run("primary", func(t *testing.T) {
		s := newStub(t)
		reset := time.Now().Add(90 * time.Second)
		s.mux.HandleFunc("GET /api/v3/repos/acme/widgets/hooks/7/deliveries", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
			writeJSON(w, http.StatusForbidden, map[string]string{"message": "API rate limit exceeded"})
		})
		c := s.client(t)
		_, err := c.ListDeliveries(context.Background(), Target{Owner: "acme", Repo: "widgets", HookID: 7}, base)
		var rl *ErrRateLimited
		require.ErrorAs(t, err, &rl)
		assert.True(t, IsRateLimited(err))
		assert.InDelta(t, 90, rl.RetryAfter.Seconds(), 5)
		assert.Contains(t, err.Error(), "rate limited")

		// go-github remembers the exhausted limit and fails fast without a
		// request; that must classify the same way.
		before := len(s.paths())
		err = c.Redeliver(context.Background(), Target{Owner: "acme", Repo: "widgets", HookID: 7}, 1)
		assert.True(t, IsRateLimited(err), "%v", err)
		assert.Len(t, s.paths(), before)
	})
	t.Run("secondary with retry-after", func(t *testing.T) {
		s := newStub(t)
		s.mux.HandleFunc("GET /api/v3/orgs/acme/hooks/3/deliveries", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "30")
			writeJSON(w, http.StatusTooManyRequests, map[string]string{
				"message":           "You have exceeded a secondary rate limit",
				"documentation_url": "https://docs.github.com/rest/overview/rate-limits-for-the-rest-api#about-secondary-rate-limits",
			})
		})
		c := s.client(t)
		_, err := c.ListDeliveries(context.Background(), Target{Org: "acme", HookID: 3}, base)
		var rl *ErrRateLimited
		require.ErrorAs(t, err, &rl)
		assert.Equal(t, 30*time.Second, rl.RetryAfter)
		assert.Contains(t, err.Error(), "retry after 30s")
	})
	t.Run("secondary without retry-after", func(t *testing.T) {
		s := newStub(t)
		s.mux.HandleFunc("GET /api/v3/orgs/acme/hooks/3/deliveries", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusForbidden, map[string]string{
				"message":           "abuse",
				"documentation_url": "https://docs.github.com/#abuse-rate-limits",
			})
		})
		c := s.client(t)
		_, err := c.ListDeliveries(context.Background(), Target{Org: "acme", HookID: 3}, base)
		var rl *ErrRateLimited
		require.ErrorAs(t, err, &rl)
		assert.Equal(t, time.Duration(0), rl.RetryAfter)
		assert.NotContains(t, err.Error(), "retry after")
		require.Error(t, rl.Unwrap())
	})
}

func TestRedeliver(t *testing.T) {
	s := newStub(t)
	s.mux.HandleFunc("POST /api/v3/repos/acme/widgets/hooks/7/deliveries/42/attempts", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	s.mux.HandleFunc("POST /api/v3/orgs/acme/hooks/3/deliveries/43/attempts", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"id": 44})
	})
	s.mux.HandleFunc("POST /api/v3/repos/acme/widgets/hooks/7/deliveries/404/attempts", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"message": "Not Found"})
	})
	c := s.client(t)

	require.NoError(t, c.Redeliver(context.Background(), Target{Owner: "acme", Repo: "widgets", HookID: 7}, 42), "202 Accepted is success")
	require.NoError(t, c.Redeliver(context.Background(), Target{Org: "acme", HookID: 3}, 43))
	err := c.Redeliver(context.Background(), Target{Owner: "acme", Repo: "widgets", HookID: 7}, 404)
	require.ErrorContains(t, err, "delivery 404")
	assert.False(t, IsRateLimited(err))
	require.ErrorContains(t, c.Redeliver(context.Background(), Target{Org: "acme"}, 1), "hook id is unknown")

	assert.Equal(t, []string{
		"POST /api/v3/repos/acme/widgets/hooks/7/deliveries/42/attempts",
		"POST /api/v3/orgs/acme/hooks/3/deliveries/43/attempts",
		"POST /api/v3/repos/acme/widgets/hooks/7/deliveries/404/attempts",
	}, s.paths())
}

func TestDiscoverHookID(t *testing.T) {
	const webhook = "https://antwatcher.example.com/webhook"
	s := newStub(t)
	// Repo hooks span two offset pages; the match is on page 2 with a
	// trailing slash and upper-case host.
	s.mux.HandleFunc("GET /api/v3/repos/acme/widgets/hooks", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "100", r.URL.Query().Get("per_page"))
		switch r.URL.Query().Get("page") {
		case "", "1":
			w.Header().Set("Link", `<https://api.github.com/repos/acme/widgets/hooks?page=2&per_page=100>; rel="next"`)
			writeJSON(w, http.StatusOK, []any{hookJSON(1, "https://other.example.com/hook"), hookJSON(2, "https://antwatcher.example.com/webhook/other")})
		case "2":
			writeJSON(w, http.StatusOK, []any{hookJSON(3, "https://ANTWATCHER.example.com/webhook/"), map[string]any{"id": 4}})
		}
	})
	s.mux.HandleFunc("GET /api/v3/orgs/acme/hooks", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, []any{hookJSON(10, webhook), hookJSON(11, webhook)})
	})
	s.mux.HandleFunc("GET /api/v3/orgs/empty/hooks", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, []any{hookJSON(20, "https://other.example.com/hook")})
	})
	s.mux.HandleFunc("GET /api/v3/orgs/forbidden/hooks", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"message": "Not Found"})
	})
	// endless always advertises a next page, so the loop exhausts maxPages
	// without ever seeing the end of the list.
	s.mux.HandleFunc("GET /api/v3/orgs/endless/hooks", func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "" {
			page = "1"
		}
		n, _ := strconv.Atoi(page)
		w.Header().Set("Link", fmt.Sprintf(`<https://api.github.com/orgs/endless/hooks?page=%d&per_page=100>; rel="next"`, n+1))
		writeJSON(w, http.StatusOK, []any{hookJSON(int64(n), "https://other.example.com/hook")})
	})
	s.mux.HandleFunc("GET /api/v3/orgs/limited/hooks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		writeJSON(w, http.StatusForbidden, map[string]string{"message": "limit"})
	})
	c := s.client(t)

	t.Run("found across pages with normalization", func(t *testing.T) {
		id, err := c.DiscoverHookID(context.Background(), Target{Owner: "acme", Repo: "widgets"}, webhook)
		require.NoError(t, err)
		assert.Equal(t, int64(3), id)
	})
	t.Run("ambiguous", func(t *testing.T) {
		_, err := c.DiscoverHookID(context.Background(), Target{Org: "acme"}, webhook)
		var amb *ErrHookAmbiguous
		require.ErrorAs(t, err, &amb)
		assert.Equal(t, []int64{10, 11}, amb.HookIDs)
		assert.Contains(t, err.Error(), "set hook_id")
	})
	t.Run("absent", func(t *testing.T) {
		_, err := c.DiscoverHookID(context.Background(), Target{Org: "empty"}, webhook)
		require.ErrorIs(t, err, ErrHookNotFound)
	})
	t.Run("exhausting pagination is not the same as absent", func(t *testing.T) {
		_, err := c.DiscoverHookID(context.Background(), Target{Org: "endless"}, webhook)
		require.ErrorContains(t, err, "pagination did not end after 500 pages")
		assert.NotErrorIs(t, err, ErrHookNotFound, "a hook may exist past the cap; do not send the operator after a missing one")
	})
	t.Run("api error", func(t *testing.T) {
		_, err := c.DiscoverHookID(context.Background(), Target{Org: "forbidden"}, webhook)
		require.ErrorContains(t, err, "Not Found")
		assert.False(t, IsRateLimited(err))
	})
	t.Run("rate limited", func(t *testing.T) {
		_, err := c.DiscoverHookID(context.Background(), Target{Org: "limited"}, webhook)
		assert.True(t, IsRateLimited(err), "%v", err)
	})
	t.Run("invalid webhook url", func(t *testing.T) {
		_, err := c.DiscoverHookID(context.Background(), Target{Org: "acme"}, "/webhook")
		require.ErrorContains(t, err, "not an absolute url")
		_, err = c.DiscoverHookID(context.Background(), Target{Org: "acme"}, "://x")
		require.ErrorContains(t, err, "webhook url")
	})
}

func TestNormalizeURL(t *testing.T) {
	a, err := normalizeURL("HTTPS://Example.com/Webhook/")
	require.NoError(t, err)
	b, err := normalizeURL("https://example.com/Webhook#frag")
	require.NoError(t, err)
	assert.Equal(t, a, b)
	c, err := normalizeURL("https://example.com/webhook")
	require.NoError(t, err)
	assert.NotEqual(t, a, c, "path stays case-sensitive")
	_, err = normalizeURL("")
	require.Error(t, err)
}

func TestClassify_PassThrough(t *testing.T) {
	err := errors.New("plain")
	assert.Same(t, err, classify(err))
}
