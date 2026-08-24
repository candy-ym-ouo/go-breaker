APP := go-breaker
DIST := dist
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
BINARY := $(DIST)/$(APP)-$(GOOS)-$(GOARCH)
ARCHIVE := $(DIST)/$(APP)-$(GOOS)-$(GOARCH).tar.gz

.PHONY: run test check build package clean

run:
	go run ./cmd/demo

test:
	go test -race ./...

check:
	@test -z "$$(gofmt -l .)"
	go vet ./...
	go test -race ./...

build:
	@mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -trimpath -ldflags="-s -w" -o $(BINARY) ./cmd/demo

package: build
	tar -czf $(ARCHIVE) -C $(DIST) $(notdir $(BINARY))
	@echo "created $(ARCHIVE)"

clean:
	@mkdir -p $(DIST)
	@find $(DIST) -mindepth 1 -maxdepth 1 -type f -delete
