.PHONY: all lint lint-check acceptance fuzz-short fuzz-long pbt-faults pbt-faults-clean pbt-memxfer pbt-tree pbt-pool vet build test unit bench bench-acceptance bench-acceptance-dials bench-throughput

FUZZTIME_SHORT ?= 5s
FUZZTIME_LONG ?= 30s
# Fuzzing often needs extra headroom beyond -fuzztime for baseline coverage,
# worker shutdown, and slower race-enabled executions in CI.
FUZZDEADLINE_SHORT ?= 30s
FUZZDEADLINE_LONG ?= 2m
# pbt-faults: each property gets longer than fuzz-long, on a few workers
# rather than every core. PBTCACHE is a fuzz cache each run starts empty:
# replaying a grown corpus of these slow inputs would spend the budget.
PBTTIME ?= 2m
PBTDEADLINE ?= 10m
PBTPARALLEL ?= 4
PBTCACHE ?= $(CURDIR)/.pbt-fuzzcache
# Fault kinds the pbt-faults swarms leave out, comma-separated, such as
# disk.write.lost,disk.write.flip: to look past failures already recorded.
# Until those are fixed, an empty list fails on its first seed; known, alone
# or in the list, stands for PBT_KNOWN, the kinds behind the failures
# docs/IDEAS.md records.
PBT_EXCLUDE ?=
PBT_KNOWN := disk.read.flip,disk.write.lost,disk.write.flip,disk.open.enoent,sys.fdatasync.fail,sys.fstat.fail,net.drop
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

# Keeps raw IO behind the fault gates (.golangci.yml, docs/arch/FAULTS.md).
# The version is pinned so CI and laptops agree; the first run builds it.
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1
# scripts/lint-nolint rejects //nolint:depguard and //nolint:all, which would
# switch the syscall ban off for a line; nolintlint cannot forbid them.
lint:
	$(GOLANGCI_LINT) run ./...
	./scripts/lint-nolint

# Proves each lint rule fires and stays quiet where it should, in a copy of the tree.
lint-check:
	./scripts/lint-check

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
	$(FUZZ) -race ./internal/aead FuzzParsePreface $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/sampler FuzzGeneratorSlots $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/utils FuzzCommonPrefixLen $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) . FuzzSuggestBatchMaxBytes $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT) -parallel=1
	$(FUZZ) -race . FuzzChecksumStreamReuse $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race . FuzzManifestEntryByID $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/cmd/filexfercli FuzzVerifyCursor $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/cmd/filexfercli FuzzChecksumFrameParsing $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/bench/dataset FuzzPlan $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)
	$(FUZZ) -race ./internal/bench/dataset FuzzSelectWarmBlocks $(FUZZTIME_SHORT) $(FUZZDEADLINE_SHORT)

# Properties still finding new coverage past 10s. CI gives them a larger
# budget to keep exploring.
fuzz-long:
	$(FUZZ) -race ./internal/filexfer/ftcp FuzzChecksumFrameMatchesWriteFrame $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/stats FuzzFitBeta $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/faults FuzzSpecRoundTrip $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race . FuzzTCPConnPool $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
	$(FUZZ) -race ./internal/aead FuzzReadRecords $(FUZZTIME_LONG) $(FUZZDEADLINE_LONG)
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

# Fault-injection properties (docs/arch/FAULTS.md). They are not part of
# acceptance: they fail until the bugs they find are fixed, and each moves to
# fuzz-long once it passes. They skip unless TX_PBT_FAULTS is set, so their
# seeds never fail make unit. TestTreeCopyUnderFaults sweeps random seeds for
# PBTTIME instead of fuzzing, since its CLI copies can outlast the fuzzer's
# 10s limit per input. Each property is its own target, so make -k
# pbt-faults runs every one past a failing one, and still fails.
pbt-faults: pbt-memxfer pbt-tree pbt-pool
pbt-faults pbt-memxfer pbt-tree pbt-pool: export TX_PBT_FAULTS := 1
pbt-faults pbt-memxfer pbt-tree pbt-pool: export TX_PBT_FAULTS_EXCLUDE := $(subst known,$(PBT_KNOWN),$(PBT_EXCLUDE))
pbt-faults-clean:
	rm -rf $(PBTCACHE)
pbt-memxfer: pbt-faults-clean
	$(FUZZ) -race ./internal/memxfer FuzzMemTransferUnderFaults $(PBTTIME) $(PBTDEADLINE) -parallel=$(PBTPARALLEL) -fuzzcachedir=$(PBTCACHE)/memxfer
pbt-tree:
	TX_PBT_FAULTS_TIME=$(PBTTIME) go test -race -count=1 -timeout $(PBTDEADLINE) -run '^TestTreeCopyUnderFaults$$' ./internal/cmd/filexfercli
pbt-pool: pbt-faults-clean
	$(FUZZ) -race . FuzzTCPConnPoolUnderFaults $(PBTTIME) $(PBTDEADLINE) -parallel=$(PBTPARALLEL) -fuzzcachedir=$(PBTCACHE)/pool

# internal/bench holds benchmarks of exported code. Benchmarks that need
# unexported access live with their package; both sets are registered here so
# the Makefile stays the single place that lists them. End-to-end benchmarks
# between hosts are tx-bench's job (docs/bench).
bench: build
	@mkdir -p bench/results
	go test -bench=. -run=^$$ -benchmem ./internal/bench ./internal/aead . ./internal/filexfer/ftcp ./internal/cmd/filexfercli | tee bench/results/latest.txt

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
