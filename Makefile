MODULE      := github.com/sokogen/antwatcher
BIN_DIR     := $(CURDIR)/bin
BINARY      := $(BIN_DIR)/antwatcher

VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE        ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

GOLANGCI_LINT_VERSION ?= v2.13.2
GOLANGCI_LINT         := $(BIN_DIR)/golangci-lint
GOLANGCI_LINT_STAMP   := $(BIN_DIR)/.golangci-lint-$(GOLANGCI_LINT_VERSION)

CONFIG      ?= antwatcher.yml

COVERAGE_PROFILE   ?= coverage.out
COVERAGE_THRESHOLD ?= 80

.PHONY: all build test lint coverage run check tidy clean readme docker

all: lint test build

build:
	@mkdir -p $(BIN_DIR)
	go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/antwatcher

test:
	go test -race -cover -coverprofile=$(COVERAGE_PROFILE) ./...

# Reads the profile `test` writes; run `make test coverage` from a clean tree.
coverage:
	COVERAGE_PROFILE=$(COVERAGE_PROFILE) sh scripts/check-coverage.sh $(COVERAGE_THRESHOLD)

# The stamp carries the version, so changing the pin reinstalls instead of
# leaving whatever binary happens to sit in $(BIN_DIR).
$(GOLANGCI_LINT_STAMP):
	@mkdir -p $(BIN_DIR)
	GOBIN=$(BIN_DIR) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	@rm -f $(BIN_DIR)/.golangci-lint-*
	@touch $@

lint: $(GOLANGCI_LINT_STAMP)
	@test -x $(GOLANGCI_LINT) || { rm -f $(GOLANGCI_LINT_STAMP); $(MAKE) $(GOLANGCI_LINT_STAMP); }
	$(GOLANGCI_LINT) run ./...

run: build
	$(BINARY) serve -config $(CONFIG)

check: build
	$(BINARY) serve -config $(CONFIG) -check

tidy:
	go mod tidy

readme:
	sh scripts/readme-config.sh

docker:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t antwatcher:$(VERSION) .

clean:
	rm -rf $(BIN_DIR) $(COVERAGE_PROFILE)
