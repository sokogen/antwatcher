package ghclient

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/go-github/v91/github"

	"github.com/sokogen/antwatcher/internal/config"
)

// PerPage is the page size used for every list call (the GitHub maximum).
const PerPage = 100

// maxPages caps one ListDeliveries or DiscoverHookID call so a misbehaving
// cursor can never loop forever: 500 pages of 100 is far beyond the 72h
// delivery window of any realistic hook.
const maxPages = 500

// Target identifies one webhook to scan: a repository hook (Owner and Repo)
// or an organization hook (Org). HookID may be zero until DiscoverHookID
// fills it.
type Target struct {
	Owner  string
	Repo   string
	Org    string
	HookID int64
}

// TargetFromConfig maps a recovery target. Repo must be "owner/name".
func TargetFromConfig(cfg config.RecoveryTarget) (Target, error) {
	t := Target{Org: cfg.Org, HookID: cfg.HookID}
	if cfg.Repo != "" {
		owner, name, ok := strings.Cut(cfg.Repo, "/")
		if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
			return Target{}, fmt.Errorf("ghclient: repo must be \"owner/name\" (got %q)", cfg.Repo)
		}
		t.Owner, t.Repo = owner, name
	}
	if t.IsOrg() == (t.Repo != "") {
		return Target{}, errors.New("ghclient: target needs exactly one of repo or org")
	}
	return t, nil
}

// IsOrg reports whether the target is an organization hook.
func (t Target) IsOrg() bool { return t.Org != "" }

// Name is the stable label of the target without the hook ID, suitable for a
// metric label: "owner/repo" or "org:name".
func (t Target) Name() string {
	if t.IsOrg() {
		return "org:" + t.Org
	}
	return t.Owner + "/" + t.Repo
}

// String renders the target with its hook ID for logs.
func (t Target) String() string {
	if t.HookID == 0 {
		return t.Name()
	}
	return fmt.Sprintf("%s#%d", t.Name(), t.HookID)
}

// Delivery is one attempt GitHub made to deliver a webhook. Attempts of the
// same event share GUID; a redelivery is a new Delivery with Redelivery set.
type Delivery struct {
	ID          int64
	GUID        string
	DeliveredAt time.Time
	StatusCode  int
	Redelivery  bool
	Event       string
	Action      string
}

// Succeeded reports whether the receiver answered 2xx.
func (d Delivery) Succeeded() bool { return d.StatusCode >= 200 && d.StatusCode <= 299 }

// ErrRateLimited is returned when GitHub applied a primary or secondary rate
// limit. RetryAfter is the wait GitHub asked for, 0 when it gave none.
type ErrRateLimited struct {
	RetryAfter time.Duration
	err        error
}

func (e *ErrRateLimited) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("ghclient: rate limited, retry after %s: %v", e.RetryAfter.Round(time.Second), e.err)
	}
	return fmt.Sprintf("ghclient: rate limited: %v", e.err)
}

// Unwrap exposes the go-github error.
func (e *ErrRateLimited) Unwrap() error { return e.err }

// IsRateLimited reports whether err is or wraps an *ErrRateLimited.
func IsRateLimited(err error) bool {
	var rl *ErrRateLimited
	return errors.As(err, &rl)
}

// ErrHookNotFound is returned by DiscoverHookID when no hook of the target
// points at the webhook URL.
var ErrHookNotFound = errors.New("ghclient: no hook matches the webhook url")

// ErrHookAmbiguous is returned by DiscoverHookID when several hooks point at
// the webhook URL; the operator must pin hook_id.
type ErrHookAmbiguous struct {
	HookIDs []int64
}

func (e *ErrHookAmbiguous) Error() string {
	return fmt.Sprintf("ghclient: %d hooks match the webhook url (ids %v); set hook_id", len(e.HookIDs), e.HookIDs)
}

// API is the subset of Client the recovery loop depends on; fakes implement it.
type API interface {
	ListDeliveries(ctx context.Context, target Target, since time.Time) ([]Delivery, error)
	Redeliver(ctx context.Context, target Target, deliveryID int64) error
	DiscoverHookID(ctx context.Context, target Target, webhookURL string) (int64, error)
}

// Client talks to the GitHub REST API. Build it with New.
type Client struct {
	gh *github.Client
}

var _ API = (*Client)(nil)

// ListDeliveries returns the delivery attempts of the target hook made at or
// after since, newest first. GitHub lists deliveries in reverse chronological
// order, so pagination stops at the first attempt older than since.
func (c *Client) ListDeliveries(ctx context.Context, target Target, since time.Time) ([]Delivery, error) {
	if target.HookID == 0 {
		return nil, fmt.Errorf("ghclient: %s: hook id is unknown", target)
	}
	var out []Delivery
	opts := &github.ListCursorOptions{PerPage: PerPage}
	for page := 0; page < maxPages; page++ {
		items, resp, err := c.listPage(ctx, target, opts)
		if err != nil {
			return nil, fmt.Errorf("ghclient: list deliveries %s: %w", target, classify(err))
		}
		for _, item := range items {
			d := fromHookDelivery(item)
			if d.DeliveredAt.Before(since) {
				return out, nil
			}
			out = append(out, d)
		}
		if len(items) == 0 || resp.Cursor == "" || resp.Cursor == opts.Cursor {
			return out, nil
		}
		opts.Cursor = resp.Cursor
	}
	return nil, fmt.Errorf("ghclient: list deliveries %s: pagination did not end after %d pages", target, maxPages)
}

func (c *Client) listPage(ctx context.Context, target Target, opts *github.ListCursorOptions) ([]*github.HookDelivery, *github.Response, error) {
	if target.IsOrg() {
		return c.gh.Organizations.ListHookDeliveries(ctx, target.Org, target.HookID, opts)
	}
	return c.gh.Repositories.ListHookDeliveries(ctx, target.Owner, target.Repo, target.HookID, opts)
}

// Redeliver asks GitHub to deliver the attempt again. GitHub answers 202 and
// records a new attempt with the same GUID; the receiver sees it as a normal
// webhook.
func (c *Client) Redeliver(ctx context.Context, target Target, deliveryID int64) error {
	if target.HookID == 0 {
		return fmt.Errorf("ghclient: %s: hook id is unknown", target)
	}
	var err error
	if target.IsOrg() {
		_, _, err = c.gh.Organizations.RedeliverHookDelivery(ctx, target.Org, target.HookID, deliveryID)
	} else {
		_, _, err = c.gh.Repositories.RedeliverHookDelivery(ctx, target.Owner, target.Repo, target.HookID, deliveryID)
	}
	var accepted *github.AcceptedError
	if err == nil || errors.As(err, &accepted) {
		return nil
	}
	return fmt.Errorf("ghclient: redeliver %s delivery %d: %w", target, deliveryID, classify(err))
}

// DiscoverHookID finds the hook of the target whose config.url is webhookURL.
// URLs are compared after normalization (case-insensitive scheme and host,
// trailing slash ignored). Exactly one match is required: ErrHookNotFound or
// *ErrHookAmbiguous otherwise.
func (c *Client) DiscoverHookID(ctx context.Context, target Target, webhookURL string) (int64, error) {
	want, err := normalizeURL(webhookURL)
	if err != nil {
		return 0, fmt.Errorf("ghclient: discover hook %s: webhook url: %w", target, err)
	}
	var matches []int64
	opts := &github.ListOptions{PerPage: PerPage}
	for page := 0; page < maxPages; page++ {
		hooks, resp, err := c.listHooks(ctx, target, opts)
		if err != nil {
			return 0, fmt.Errorf("ghclient: discover hook %s: %w", target, classify(err))
		}
		for _, h := range hooks {
			got, err := normalizeURL(h.GetConfig().GetURL())
			if err == nil && got == want {
				matches = append(matches, h.GetID())
			}
		}
		if resp.NextPage == 0 || resp.NextPage == opts.Page {
			break
		}
		opts.Page = resp.NextPage
	}
	switch len(matches) {
	case 0:
		return 0, fmt.Errorf("ghclient: discover hook %s: %w", target, ErrHookNotFound)
	case 1:
		return matches[0], nil
	default:
		return 0, fmt.Errorf("ghclient: discover hook %s: %w", target, &ErrHookAmbiguous{HookIDs: matches})
	}
}

func (c *Client) listHooks(ctx context.Context, target Target, opts *github.ListOptions) ([]*github.Hook, *github.Response, error) {
	if target.IsOrg() {
		return c.gh.Organizations.ListHooks(ctx, target.Org, opts)
	}
	return c.gh.Repositories.ListHooks(ctx, target.Owner, target.Repo, opts)
}

// classify maps go-github rate limit errors to *ErrRateLimited and leaves
// every other error untouched.
func classify(err error) error {
	var primary *github.RateLimitError
	if errors.As(err, &primary) {
		var wait time.Duration
		if reset := primary.Rate.Reset.Time; !reset.IsZero() {
			wait = max(time.Until(reset), 0)
		}
		return &ErrRateLimited{RetryAfter: wait, err: err}
	}
	var secondary *github.AbuseRateLimitError
	if errors.As(err, &secondary) {
		return &ErrRateLimited{RetryAfter: max(secondary.GetRetryAfter(), 0), err: err}
	}
	return err
}

func fromHookDelivery(h *github.HookDelivery) Delivery {
	return Delivery{
		ID:          h.GetID(),
		GUID:        h.GetGUID(),
		DeliveredAt: h.GetDeliveredAt().Time,
		StatusCode:  h.GetStatusCode(),
		Redelivery:  h.GetRedelivery(),
		Event:       h.GetEvent(),
		Action:      h.GetAction(),
	}
}

// normalizeURL lowercases scheme and host and drops a trailing slash so
// operator-typed and GitHub-stored URLs compare equal.
func normalizeURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("%q is not an absolute url", raw)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.RawPath = ""
	u.Fragment = ""
	return u.String(), nil
}
