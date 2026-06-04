STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.7.0
GOVULNCHECK := golang.org/x/vuln/cmd/govulncheck@v1.3.0

.DEFAULT_GOAL := help
.PHONY: check fmt fmt-fix build vet test race staticcheck vuln fuzz-smoke dist tidy-check \
		install install-server install-client uninstall help

VERSION := $(shell git describe --tags --match 'v*' --always --dirty 2>/dev/null || echo dev)

check: fmt build vet test race staticcheck vuln ## run the full gate (== "this commit is good")

fmt: ## fail if any file is not gofmt-clean (gofmt -l exits 0 even when it lists files)
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then printf 'gofmt needed:\n%s\n' "$$out"; exit 1; fi

fmt-fix: ## gofmt -w the whole tree
	gofmt -w .

build: ## compile every package (this is what makes the os.Root build-time guard bite)
	go build ./...

dist: ## build the stamped, static release binary into bin/
	@mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/buff ./cmd/buff

vet: ## go vet
	go vet ./...

test: ## run tests
	go test ./...

race: ## run tests under the race detector (needs CGO + a C compiler)
	go test -race ./...

staticcheck: ## static analysis (pinned; must support the Go version in go.mod)
	go run $(STATICCHECK) ./...

vuln: ## vulnerability scan (needs network; catches stdlib advisories even with zero deps)
	go run $(GOVULNCHECK) ./...

fuzz-smoke: ## time-boxed fuzz of the validator surfaces (one invocation per target)
	go test -run='^$$' -fuzz='^FuzzValidName$$' -fuzztime=10s ./clip/
	go test -run='^$$' -fuzz='^FuzzValidFilename$$' -fuzztime=10s ./clip/
	go test -run='^$$' -fuzz='^FuzzExtractPath$$' -fuzztime=10s ./archive/
	go test -run='^$$' -fuzz='^FuzzFilenameCodec$$' -fuzztime=10s ./api/

tidy-check: ## additive guard; only meaningful once the module has dependencies. Not part of `check`.
	go mod tidy
	git diff --exit-code -- go.mod go.sum

install: dist ## install the buff server: binary + config + service (alias for install-server)
	@$(MAKE) --no-print-directory -C etc install BUILT_BIN=$(CURDIR)/bin/buff

install-server: dist ## install the buff server: binary + config + host-OS service (delegates to etc/)
	@$(MAKE) --no-print-directory -C etc install-server BUILT_BIN=$(CURDIR)/bin/buff

install-client: dist ## install just the buff binary, client-only (delegates to etc/)
	@$(MAKE) --no-print-directory -C etc install-client BUILT_BIN=$(CURDIR)/bin/buff

uninstall: ## remove the installed binary + host-OS service (delegates to etc/)
	@$(MAKE) --no-print-directory -C etc uninstall

help: ## list available targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  %-14s %s\n", $$1, $$2}'
