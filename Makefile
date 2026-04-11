.PHONY: all acceptance build test unit bench

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

bench: build
	@mkdir -p bench/results
	go test -bench=. -run=^$$ -benchmem ./internal/bench | tee bench/results/latest.txt
	@echo
	@go run ./internal/bench report bench/results/latest.txt
