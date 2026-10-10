.PHONY: all acceptance fuzz-short fuzz-long vet build test unit bench bench-acceptance bench-acceptance-dials bench-throughput

FUZZTIME_SHORT ?= 5s
FUZZTIME_LONG ?= 30s
# Fuzzing often needs extra headroom beyond -fuzztime for baseline coverage,
# worker shutdown, and slower race-enabled executions in CI.
FUZZDEADLINE_SHORT ?= 30s
FUZZDEADLINE_LONG ?= 2m
# Fuzzes one target for a time budget, ending it with SIGINT rather than
# -fuzztime (see scripts/fuzz): [-race] PACKAGE TARGET DURATION DEADLINE.
FUZZ := ./scripts/fuzz
# bench-acceptance: dataset size and where its metrics land.
BENCH_SIZE ?= 5GiB
BENCH_OUT ?= bench/acceptance
THROUGHPUT_SIZE ?= 256MiB
# Comma-separated server CPU counts; empty covers 2, 4, 8, ... up to the host.
THROUGHPUT_CPUS ?=
THROUGHPUT_OUT ?= bench/throughput
# Session encryption modes to measure: none, aes, chacha20.
THROUGHPUT_ENCRYPT ?= none,aes

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
# scripts/fuzz fails when a package and target fuzz nothing, which plain
# `go test -fuzz=X` would let pass silently.
acceptance:
	$(MAKE) fuzz-short
	$(MAKE) fuzz-long

# Unit-test replacements: one function or invariant over a small input space.
# Their corpus saturates well before 10s, so a longer budget buys nothing.
fuzz-short:
	$(FUZZ) -race ./internal/aead FuzzRoundTrip $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/sampler FuzzGeneratorSlots $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/utils FuzzCommonPrefixLen $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) . FuzzSuggestBatchMaxBytes $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT) -parallel=1
	$(FUZZ) -race . FuzzChecksumStreamReuse $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race . FuzzManifestEntryByID $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/cmd/filexfercli FuzzVerifyCursor $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/bench/dataset FuzzPlan $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/bench/dataset FuzzSelectWarmBlocks $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)

# End-to-end properties driving the whole system. These are still finding new
# coverage past 10s, so CI gives them a larger budget to keep exploring.
fuzz-long:
	$(FUZZ) -race . FuzzTCPConnPool $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/filexfer/encoding FuzzManifestEntryRoundTrip $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/filexfer/ftcp FuzzFramedItemRoundTrip $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/filexfer/ftcp FuzzFramedBodyHeader $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/filexfer/ftcp FuzzSync $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/filexfer/ftcp FuzzResolveUnderRoot $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/filexfer/ftcp FuzzServeZeroCopySEND $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG) -parallel=1
	$(FUZZ) -race ./internal/filexfer/store FuzzStoreStateCounts $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/events FuzzAppendJSON $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/bench/report FuzzTraceRecordText $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/bench/dataset FuzzFilesTSVRoundTrip $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/bench/dataset FuzzVerifyDetectsCorruption $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/cmd/filexfercli FuzzCopyVerifyDetectsCorruption $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG) -parallel=4

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

# Checks the dials of the last bench-acceptance run.
bench-acceptance-dials:
	TX_BENCH_ACCEPTANCE_OUT=$(abspath $(BENCH_OUT)) \
		go test -count=1 -run '^TestBenchAcceptanceDialBudget$$' -v ./internal/bench/harness

# Copies THROUGHPUT_SIZE at file sizes from 4 KiB to 16 MiB through a real
# server and client in one process, with no filesystem, once per server CPU
# count up to the host's and per THROUGHPUT_ENCRYPT mode, and logs the size x
# CPU grids: cost ratio, speedup, CPU, and rate. Fails when small files cost
# more than TX_BENCH_THROUGHPUT_MAX_RATIO times the CPU of 16 MiB files for the
# same bytes, and writes THROUGHPUT_OUT/throughput.json.
bench-throughput:
	TX_BENCH_THROUGHPUT_SIZE=$(THROUGHPUT_SIZE) TX_BENCH_THROUGHPUT_CPUS=$(THROUGHPUT_CPUS) \
		TX_BENCH_THROUGHPUT_ENCRYPT=$(THROUGHPUT_ENCRYPT) \
		TX_BENCH_THROUGHPUT_OUT=$(abspath $(THROUGHPUT_OUT)) \
		go test -count=1 -timeout 10m -run '^TestTransferCostRatio$$' -v ./internal/bench
