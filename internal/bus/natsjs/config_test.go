package natsjs

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sokogen/antwatcher/internal/config"
)

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	require.NoError(t, cfg.Validate())
	require.NoError(t, cfg.ValidateWith(config.Default().Router), "defaults satisfy ack_wait > process_timeout + margin")
	assert.True(t, cfg.Embedded)
	assert.Equal(t, "./data/nats", cfg.StoreDir)
	assert.Equal(t, "ANTWATCHER", cfg.Stream)
	assert.Equal(t, 7*24*time.Hour, cfg.Retention)
	assert.Equal(t, 2*time.Hour, cfg.DedupWindow)
	assert.Equal(t, 90*time.Second, cfg.AckWait)
	assert.Equal(t, 5*time.Second, cfg.NakDelayMin)
	assert.Equal(t, 5*time.Minute, cfg.NakDelayMax)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string // substring of the error, "" for valid
	}{
		{"defaults", func(*Config) {}, ""},
		{"external url", func(c *Config) { c.Embedded = false; c.URL = "nats://h:4222"; c.StoreDir = "" }, ""},
		{"dedup disabled", func(c *Config) { c.DedupWindow = 0 }, ""},
		{"embedded without store_dir", func(c *Config) { c.StoreDir = "" }, "store_dir is required"},
		{"external without url", func(c *Config) { c.Embedded = false }, "url is required"},
		{"empty stream", func(c *Config) { c.Stream = "" }, "stream is required"},
		{"stream with dot", func(c *Config) { c.Stream = "a.b" }, "must match"},
		{"stream with space", func(c *Config) { c.Stream = "a b" }, "must match"},
		{"retention too small", func(c *Config) { c.Retention = 50 * time.Millisecond }, "retention must be >="},
		{"retention zero", func(c *Config) { c.Retention = 0 }, "retention must be >="},
		{"negative dedup", func(c *Config) { c.DedupWindow = -time.Second }, "dedup_window must be >= 0"},
		{"dedup too small", func(c *Config) { c.DedupWindow = time.Millisecond }, "dedup_window must be 0 or >="},
		{"dedup exceeds retention", func(c *Config) { c.Retention = time.Hour; c.DedupWindow = 2 * time.Hour }, "must not exceed retention"},
		{"ack_wait zero", func(c *Config) { c.AckWait = 0 }, "ack_wait must be > 0"},
		{"nak min zero", func(c *Config) { c.NakDelayMin = 0 }, "nak_delay_min must be > 0"},
		{"nak max below min", func(c *Config) { c.NakDelayMin = time.Minute; c.NakDelayMax = time.Second }, "nak_delay_max"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Stream = ""
	cfg.AckWait = 0
	cfg.NakDelayMin = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Equal(t, 3, strings.Count(err.Error(), "\n")+1, "all three problems are listed: %v", err)
}

func TestValidateWithJoinsErrors(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Stream = ""
	cfg.AckWait = time.Second
	err := cfg.ValidateWith(config.Router{ProcessTimeout: time.Minute})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stream is required")
	assert.Contains(t, err.Error(), "ack_wait (1s) must exceed router.process_timeout + 10s (1m10s)")
}

func TestCredentialsOption(t *testing.T) {
	opt, err := credentialsOption("")
	require.NoError(t, err)
	assert.Nil(t, opt, "no credentials")

	opt, err = credentialsOption("  \n")
	require.NoError(t, err)
	assert.Nil(t, opt, "blank credentials")

	opt, err = credentialsOption("/etc/nats/user.creds")
	require.NoError(t, err)
	assert.NotNil(t, opt, "a path is passed to nats.UserCredentials")

	_, err = credentialsOption("-----BEGIN NATS USER JWT-----\ngarbage\n------END NATS USER JWT------\n")
	require.Error(t, err, "decorated content without a valid JWT and seed")
	assert.Contains(t, err.Error(), "credentials")
}

func TestCredentialsOption_DecoratedJWTAndSeed(t *testing.T) {
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	seed, err := kp.Seed()
	require.NoError(t, err)

	creds := fmt.Sprintf(
		"-----BEGIN NATS USER JWT-----\n%s\n------END NATS USER JWT------\n\n"+
			"-----BEGIN USER NKEY SEED-----\n%s\n------END USER NKEY SEED------\n",
		"eyJhbGciOiJlZDI1NTE5In0.eyJzdWIiOiJ0ZXN0In0.fake-signature", seed)

	opt, err := credentialsOption(creds)
	require.NoError(t, err, "a real decorated JWT and NKey seed parse end to end")
	assert.NotNil(t, opt, "produces a nats.UserJWTAndSeed option")
}
