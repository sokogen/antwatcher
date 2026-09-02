// Package ghclient wraps go-github for the two things recovery needs from the
// GitHub REST API: listing webhook deliveries of a repository or organization
// hook and asking GitHub to redeliver one. It never touches the Actions API.
//
// Errors are classified for the caller: a primary or secondary rate limit is
// returned as *ErrRateLimited (with the wait GitHub asked for), hook discovery
// distinguishes ErrHookNotFound from *ErrHookAmbiguous, and everything else is
// the go-github error wrapped with the target for context.
package ghclient

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/go-github/v88/github"

	"github.com/sokogen/antwatcher/internal/config"
)

// ErrUnsupportedAuth is returned by New for an auth type the schema reserves
// but this build does not implement (github_app).
var ErrUnsupportedAuth = errors.New("ghclient: unsupported auth type")

// Auth selects how the GitHub API is authenticated. Only config.AuthTypeToken
// is implemented; config.AuthTypeGitHubApp is recognized and rejected with
// ErrUnsupportedAuth so the config schema can reserve it.
type Auth struct {
	Type  string
	Token config.Secret
}

// AuthFromConfig maps the recovery auth block to Auth.
func AuthFromConfig(cfg config.RecoveryAuth) Auth {
	return Auth{Type: cfg.Type, Token: cfg.Token}
}

// Validate reports whether a is usable by New without building a client.
func (a Auth) Validate() error {
	switch a.Type {
	case config.AuthTypeToken:
		if a.Token.Reveal() == "" {
			return errors.New("ghclient: auth token is empty")
		}
		return nil
	case config.AuthTypeGitHubApp:
		return fmt.Errorf("%w: %q is reserved and not implemented yet", ErrUnsupportedAuth, a.Type)
	case "":
		return errors.New("ghclient: auth type is required")
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedAuth, a.Type)
	}
}

// DefaultTimeout bounds one HTTP round trip when Options.Timeout is zero.
const DefaultTimeout = 30 * time.Second

// Options tunes the client. The zero value targets github.com.
type Options struct {
	// APIBaseURL is the API root for GitHub Enterprise Server, for example
	// https://ghe.example.com or https://ghe.example.com/api/v3/ (the api/v3
	// suffix is added when missing). Empty means https://api.github.com/.
	APIBaseURL string
	// Timeout bounds one HTTP round trip; DefaultTimeout when zero. Callers
	// bound whole operations with the context.
	Timeout time.Duration
	// UserAgent overrides the User-Agent header; "antwatcher" when empty.
	UserAgent string
	// HTTPClient replaces the transport; nil uses http.DefaultTransport.
	// Its timeout is overridden by Timeout.
	HTTPClient *http.Client
}

// New builds an authenticated client. It performs no network call; a bad token
// surfaces as a 401 on the first request. Only token auth is implemented:
// any other type returns ErrUnsupportedAuth.
func New(auth Auth, opts Options) (*Client, error) {
	if err := auth.Validate(); err != nil {
		return nil, err
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	userAgent := opts.UserAgent
	if userAgent == "" {
		userAgent = "antwatcher"
	}
	ghOpts := []github.ClientOptionsFunc{
		github.WithAuthToken(auth.Token.Reveal()),
		github.WithTimeout(timeout),
		github.WithUserAgent(userAgent),
	}
	if opts.HTTPClient != nil {
		ghOpts = append(ghOpts, github.WithHTTPClient(opts.HTTPClient))
	}
	if opts.APIBaseURL != "" {
		ghOpts = append(ghOpts, github.WithEnterpriseURLs(opts.APIBaseURL, opts.APIBaseURL))
	}
	gh, err := github.NewClient(ghOpts...)
	if err != nil {
		return nil, fmt.Errorf("ghclient: %w", err)
	}
	return &Client{gh: gh}, nil
}
