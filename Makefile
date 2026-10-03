.PHONY: all acceptance fuzz-short fuzz-long vet build test unit bench bench-acceptance bench-acceptance-dials

FUZZTIME_SHORT ?= 5s
FUZZTIME_LONG ?= 30s
# Fuzzing often needs extra headroom beyond -fuzztime for baseline coverage,
# worker shutdown, and slower race-enabled executions in CI.
FUZZDEADLINE_SHORT ?= 30s
FUZZDEADLINE_LONG ?= 2m
# bench-acceptance: dataset size and where its metrics land.
BENCH_SIZE ?= 5GiB
BENCH_OUT ?= bench/acceptance

all: build test

vet:
	go vet ./...

# Both binaries land in the repo root; tx-bench forks the tx next to it.
build: vet
	CGO_ENABLED=0 go build -tags netgo -ldflags='-s -w -extldflags "-static"' -o tx ./cmd/tx
	CGO_ENABLED=0 go build -tags netgo -ldflags='-s -w -extldflags "-static"' -o tx-bench ./cmd/tx-bench

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
	go test -race ./internal/bench/dataset  -run=^$$ -fuzz=FuzzPlan -fuzztime=$(FUZZTIME_SHORT) -timeout=$(FUZZDEADLINE_SHORT)
	go test -race ./internal/bench/dataset  -run=^$$ -fuzz=FuzzSelectWarmBlocks -fuzztime=$(FUZZTIME_SHORT) -timeout=$(FUZZDEADLINE_SHORT)

# End-to-end properties driving the whole system. These are still finding new
# coverage past 10s, so CI gives them a larger budget to keep exploring.
fuzz-long:
	go test -race ./internal/filexfer/encoding -run=^$$ -fuzz=FuzzManifestEntryRoundTrip -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG)
	go test -race ./internal/filexfer/ftcp -run=^$$ -fuzz=FuzzFramedItemRoundTrip -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG)
	go test -race ./internal/filexfer/ftcp -run=^$$ -fuzz=FuzzFramedBodyHeader -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG)
	go test -race ./internal/filexfer/ftcp -run=^$$ -fuzz=FuzzSync -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG)
	go test -race ./internal/filexfer/ftcp -run=^$$ -fuzz=FuzzResolveUnderRoot -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG)
	go test -race ./internal/filexfer/ftcp -run=^$$ -fuzz=FuzzServeZeroCopySEND -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG) -parallel=1
	go test -race ./internal/events        -run=^$$ -fuzz=FuzzAppendJSON -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG)
	go test -race ./internal/bench/report  -run=^$$ -fuzz=FuzzTraceRecordText -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG)
	go test -race ./internal/bench/dataset -run=^$$ -fuzz=FuzzFilesTSVRoundTrip -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG)
	go test -race ./internal/bench/dataset -run=^$$ -fuzz=FuzzVerifyDetectsCorruption -fuzztime=$(FUZZTIME_LONG) -timeout=$(FUZZDEADLINE_LONG)

# internal/bench holds benchmarks of exported code. Benchmarks that need
# unexported access live with their package; both sets are registered here so
# the Makefile stays the single place that lists them. End-to-end benchmarks
# between hosts are tx-bench's job (docs/bench).
bench: build
	@mkdir -p bench/results
	go test -bench=. -run=^$$ -benchmem ./internal/bench . ./internal/filexfer/ftcp | tee bench/results/latest.txt

# Runs tx-bench remote send-tree and recv-copy as separate processes on a
# BENCH_SIZE dataset and checks correctness, liveness and shutdown, and
# resource budgets. The dataset and one receiver copy need about 2.5x
# BENCH_SIZE of free disk; set TX_BENCH_ACCEPTANCE_DIR to pick the disk.
bench-acceptance:
	TX_BENCH_ACCEPTANCE_SIZE=$(BENCH_SIZE) TX_BENCH_ACCEPTANCE_OUT=$(abspath $(BENCH_OUT)) \
		go test -count=1 -timeout 45m -run '^TestBenchAcceptance$$' -v ./internal/bench/harness

# Checks the dials of the last bench-acceptance run. Expected to fail until
# the client stops dialing a connection per request.
bench-acceptance-dials:
	TX_BENCH_ACCEPTANCE_OUT=$(abspath $(BENCH_OUT)) \
		go test -count=1 -run '^TestBenchAcceptanceDialBudget$$' -v ./internal/bench/harness
