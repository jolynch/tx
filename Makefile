.PHONY: all acceptance fuzz-short fuzz-long vet build build-bench test unit bench

FUZZTIME_SHORT ?= 5s
FUZZTIME_LONG ?= 30s
# Fuzzing often needs extra headroom beyond -fuzztime for baseline coverage,
# worker shutdown, and slower race-enabled executions in CI.
FUZZDEADLINE_SHORT ?= 30s
FUZZDEADLINE_LONG ?= 2m

all: build build-bench test

vet:
	go vet ./...

build: vet
	CGO_ENABLED=0 go build -tags netgo -ldflags='-s -w -extldflags "-static"' -o tx ./cmd/tx

build-bench: build
	@mkdir -p bench
	go build -o bench/bench ./internal/bench
	cp tx bench/tx

test: build unit acceptance

unit:
	go test -race ./...

# Every fuzz test in the repo must be listed in fuzz-short or fuzz-long. To
# pick a tier, probe the new test with -fuzztime=10s: if it is still reporting
# `new interesting:` at 10s it belongs in fuzz-long, otherwise fuzz-short.
#
# Note that `go test -fuzz=X` against a package with no matching target exits 0
# without fuzzing anything, so a wrong package path here silently disables
# coverage rather than failing.
acceptance:
	$(MAKE) fuzz-short
	$(MAKE) fuzz-long

# Unit-test replacements: one function or invariant over a small input space.
# Their corpus saturates well before 10s, so a longer budget buys nothing.
fuzz-short:
	go test -race ./internal/aead           -run=^$$ -fuzz=FuzzRoundTrip -fuzztime=$(FUZZTIME_SHORT) -timeout=$(FUZZDEADLINE_SHORT)
	go test -race ./internal/sampler        -run=^$$ -fuzz=FuzzGeneratorFullCoverageNoRepeats -fuzztime=$(FUZZTIME_SHORT) -timeout=$(FUZZDEADLINE_SHORT)
	go test -race ./internal/utils          -run=^$$ -fuzz=FuzzCommonPrefixLen -fuzztime=$(FUZZTIME_SHORT) -timeout=$(FUZZDEADLINE_SHORT)
	go test .                               -run=^$$ -fuzz=FuzzSuggestBatchMaxBytes -fuzztime=$(FUZZTIME_SHORT) -timeout=$(FUZZDEADLINE_SHORT) -parallel=1

# End-to-end properties driving the whole system. These are still finding new
# coverage past 10s, so CI gives them a larger budget to keep exploring.
fuzz-long:
	go test -race ./internal/filexfer/ftcp -run=^$$ -fuzz=FuzzSync -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG)

bench: build
	@mkdir -p bench/results
	go test -bench=. -run=^$$ -benchmem ./internal/bench | tee bench/results/latest.txt
	@echo
	@go run ./internal/bench report bench/results/latest.txt
