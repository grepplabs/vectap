SHELL := /usr/bin/env bash
.SHELLFLAGS += -o pipefail -O extglob
.DEFAULT_GOAL := help

ROOT_DIR       = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
VERSION       ?= $(shell git describe --tags --always --dirty)
LDFLAGS       ?= -X github.com/grepplabs/vectap/internal/cli/version.Version=$(VERSION)


##@ General

.PHONY: help
help: ## display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

## Tool Binaries
GO_RUN := go run
GOLANGCI_LINT_VERSION := v2.11.2
GOLANGCI_LINT ?= $(GO_RUN) github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: lint
lint: ## run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: ## run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: ## verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify


##@ Development

.PHONY: fmt
fmt: ## run go fmt against code
	go fmt ./...

.PHONY: vet
vet: ## run go vet against code
	go vet ./...

.PHONY: tidy
tidy: ## run go mod tidy
	go mod tidy

##@ Build

.PHONY: build
build: ## build binary
	CGO_ENABLED=0 go build -gcflags "all=-N -l" -ldflags "$(LDFLAGS)" -o vectap ./cmd/vectap

.PHONY: clean
clean: ## clean
	@rm -f vectap


##@ Test targets

test: ## run tests
	go test -race -count=1 ./...


##@ Run targets

run: ## run service
	go run ./cmd/vectap tap -n vector -l app.kubernetes.io/name=vector --type kubernetes
