.PHONY: all acceptance build test unit

FUZZTIME ?= 5s

all: build test

build:
	CGO_ENABLED=0 go build -a -tags netgo -ldflags='-s -w -extldflags "-static"' -o tx ./cmd/tx

test: build unit acceptance

unit:
	go test -race ./...

acceptance:
	go test -race ./internal/filexfer/encoding -run=^$$ -fuzz=FuzzRoundTrip -fuzztime=$(FUZZTIME)
	go test -race ./internal/filexfer/ftcp     -run=^$$ -fuzz=FuzzSync      -fuzztime=$(FUZZTIME)
	go test -race ./internal/utils             -run=^$$ -fuzz=FuzzCommonPrefixLen -fuzztime=$(FUZZTIME)
	go test -race .                            -run=^$$ -fuzz=FuzzSuggestBatchMaxBytes -fuzztime=$(FUZZTIME)
