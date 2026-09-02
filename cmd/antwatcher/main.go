// Command antwatcher turns GitHub Actions webhooks into traces, logs, analytics
// records, archives, and forwarded events.
//
//	antwatcher serve -config antwatcher.yml [-check] [-log-level info] [-log-format json]
//	antwatcher version
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/sokogen/antwatcher/internal/config"
)

// Set at build time through -ldflags (see Makefile).
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

const usageText = `usage: antwatcher <command> [flags]

commands:
  serve     load the configuration and run the service (-check validates and exits)
  version   print version information

run "antwatcher serve -h" for serve flags
`

// run executes the CLI and returns the process exit code: 0 ok, 1 runtime or
// configuration failure, 2 usage error.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	switch args[0] {
	case "serve":
		return runServe(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintf(stdout, "antwatcher %s (commit %s, built %s)\n", version, commit, date)
		return 0
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usageText)
		return 0
	default:
		fmt.Fprintf(stderr, "antwatcher: unknown command %q\n\n%s", args[0], usageText)
		return 2
	}
}

func runServe(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "antwatcher.yml", "path to the configuration file")
	check := fs.Bool("check", false, "load and validate the configuration, print it with secrets masked, and exit")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, error")
	logFormat := fs.String("log-format", "json", "log format: json or text")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "serve: unexpected arguments: %s\n", strings.Join(fs.Args(), " "))
		return 2
	}

	logger, err := newLogger(stderr, *logLevel, *logFormat)
	if err != nil {
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return 2
	}

	// Bus and sink drivers register their Describers here in later tasks so
	// -check can render and cross-validate their blocks.
	describers := config.Describers{}

	redacted, err := loadConfig(*configPath, describers)
	if err != nil {
		if *check {
			fmt.Fprintf(stderr, "configuration is invalid:\n%v\n", err)
		} else {
			logger.Error("configuration is invalid", "config", *configPath, "error", err.Error())
		}
		return 1
	}

	if *check {
		out, err := yaml.Marshal(redacted)
		if err != nil {
			fmt.Fprintf(stderr, "render configuration: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "# %s: configuration is valid (secrets masked)\n", *configPath)
		_, _ = stdout.Write(out)
		return 0
	}

	logger.Info("configuration loaded",
		"config", *configPath,
		"version", version,
		"bus_driver", redacted.Bus.Driver,
		"sinks", len(redacted.Sinks),
		"recovery_enabled", redacted.Recovery.Enabled,
		"settings", redacted,
	)
	logger.Warn("serve is not implemented yet: the configuration was loaded and nothing was started")
	return 0
}

// loadConfig loads, validates, and redacts the configuration at path. All
// validation errors are returned together so an operator fixes them in one pass.
func loadConfig(path string, describers config.Describers) (config.RedactedConfig, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return config.RedactedConfig{}, err
	}
	if err := cfg.Validate(); err != nil {
		return config.RedactedConfig{}, err
	}
	if err := config.ValidateDrivers(cfg, describers); err != nil {
		return config.RedactedConfig{}, err
	}
	return config.Redacted(cfg, describers)
}

func newLogger(w io.Writer, level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid -log-level %q: use debug, info, warn, or error", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("invalid -log-format %q: use json or text", format)
	}
}
