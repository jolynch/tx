.PHONY: all acceptance build build-bench test unit bench

FUZZTIME ?= 5s
# Fuzzing often needs extra headroom beyond -fuzztime for baseline coverage,
# worker shutdown, and slower race-enabled executions in CI.
FUZZDEADLINE ?= 30s

all: build build-bench test

build:
	CGO_ENABLED=0 go build -a -tags netgo -ldflags='-s -w -extldflags "-static"' -o tx ./cmd/tx

build-bench: build
	@mkdir -p bench
	go build -o bench/bench ./internal/bench
	cp tx bench/tx

test: build unit acceptance

unit:
	go test -race ./...

acceptance:
	go test -race ./internal/filexfer/encoding -run=^$$ -fuzz=FuzzRoundTrip -fuzztime=$(FUZZTIME) -timeout=$(FUZZDEADLINE)
	go test -race ./internal/filexfer/ftcp     -run=^$$ -fuzz=FuzzSync      -fuzztime=$(FUZZTIME) -timeout=$(FUZZDEADLINE)
	go test -race ./internal/utils             -run=^$$ -fuzz=FuzzCommonPrefixLen -fuzztime=$(FUZZTIME) -timeout=$(FUZZDEADLINE)
	go test -race .                            -run=^$$ -fuzz=FuzzSuggestBatchMaxBytes -fuzztime=$(FUZZTIME) -timeout=$(FUZZDEADLINE)

bench: build
	@mkdir -p bench/results
	go test -bench=. -run=^$$ -benchmem ./internal/bench | tee bench/results/latest.txt
	@echo
	@go run ./internal/bench report bench/results/latest.txt
