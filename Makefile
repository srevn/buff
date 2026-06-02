STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.7.0
GOVULNCHECK := golang.org/x/vuln/cmd/govulncheck@v1.3.0

.DEFAULT_GOAL := help
.PHONY: check fmt fmt-fix build vet test race staticcheck vuln fuzz-smoke build-freebsd dist tidy-check help

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

test: ## run tests (includes the synctest toolchain guard)
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

build-freebsd: ## prove the tree compiles for the freebsd server target (no runner gates it at runtime)
	GOOS=freebsd GOARCH=amd64 go build ./...

tidy-check: ## additive guard; only meaningful once the module has dependencies. Not part of `check`.
	go mod tidy
	git diff --exit-code -- go.mod go.sum

help: ## list available targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  %-12s %s\n", $$1, $$2}'
