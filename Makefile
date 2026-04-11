.PHONY: all build test fuzz

FUZZTIME ?= 5s

all: build test fuzz

build:
	CGO_ENABLED=0 go build -a -tags netgo -ldflags='-s -w -extldflags "-static"' -o tx ./server

test: build
	go test -race ./...

fuzz:
	go test ./server/internal/filexfer/encoding -run=^$$ -fuzz=FuzzRoundTrip -fuzztime=$(FUZZTIME)
	go test ./server/internal/filexfer/ftcp     -run=^$$ -fuzz=FuzzSync      -fuzztime=$(FUZZTIME)
	go test ./server/utils                      -run=^$$ -fuzz=FuzzCommonPrefixLen -fuzztime=$(FUZZTIME)
	go test ./server/filexfer                   -run=^$$ -fuzz=FuzzSuggestBatchMaxBytes -fuzztime=$(FUZZTIME)
