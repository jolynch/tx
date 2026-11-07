"""Conceptual cloud figures for docs/arch/CLOUD.md; --plot renders them.

Every figure is a deterministic model (piecewise rates over time, no
randomness) built from published AWS limits and a few assumed values. Run
`python3 docs/arch/cloud_figures.py --plot` from any directory; each figure
lands in `plots/` next to this script as a 300 dpi PNG; two runs produce
byte-identical files.
"""

import argparse
from pathlib import Path
from statistics import NormalDist

import numpy as np

DEST = Path(__file__).resolve().parent
PLOTS = DEST / "plots"

# ---------------------------------------------------------------------------
# Parameters. Published values cite their AWS source; "# assumed" values are
# modeling choices, not measurements.
# ---------------------------------------------------------------------------

GIB = 1024.0  # MiB per GiB


def gbps_to_mibps(gbps):
    """Convert decimal Gbps to MiB/s."""
    return gbps * 1e9 / 8 / (1024 * 1024)


# i4i sizes: vCPU, RAM GiB, network baseline Gbps, network burst Gbps, local
# NVMe device count. Sizes with no burst have burst == baseline.
# https://docs.aws.amazon.com/ec2/latest/instancetypes/so.html
#   (Performance, Network, Instance store tables)
I4I = {
    "large":    dict(vcpu=2,   ram=16,   net_base=0.781, net_burst=10.0, nvme=1),
    "xlarge":   dict(vcpu=4,   ram=32,   net_base=1.875, net_burst=10.0, nvme=1),
    "2xlarge":  dict(vcpu=8,   ram=64,   net_base=4.687, net_burst=12.0, nvme=1),
    "4xlarge":  dict(vcpu=16,  ram=128,  net_base=9.375, net_burst=25.0, nvme=1),
    "8xlarge":  dict(vcpu=32,  ram=256,  net_base=18.75, net_burst=18.75, nvme=2),
    "16xlarge": dict(vcpu=64,  ram=512,  net_base=37.5,  net_burst=37.5, nvme=4),
    "32xlarge": dict(vcpu=128, ram=1024, net_base=75.0,  net_burst=75.0, nvme=8),
}
I4I_XLARGE_NVME_GIB = 937e9 / 2**30   # one 937 GB device, published (so.html)
# The illustrative transfer uses an i4i.4xlarge sender and i7i.4xlarge receiver.
# Both have 16 vCPUs, 128 GiB RAM, 9.375/25 Gbps baseline/burst networking,
# and one 3,750 GB local SSD. Netflix catalogs the same baseline and drives:
# https://github.com/Netflix-Skunkworks/service-capacity-modeling/tree/main/service_capacity_modeling/hardware/profiles/shapes/aws
I7I_4XLARGE = dict(vcpu=16, ram=128, net_base=9.375, net_burst=25.0, nvme=1)
LOCAL_4XLARGE_GIB = 3750e9 / 2**30
COPY_FRACTION = .80
# Nominal sequential measurements, not universal hardware maxima. The transfer
# assumes cold sender reads. Neither source establishes an 80%-full write rate;
# keep occupancy separate from throughput and do not invent a derating.
# https://arxiv.org/html/2301.03734v1#S3.SS1 (fio, 2.9 GB/s reads)
# AWS independently lists 2,800 MB/s sequential reads on i4i.4xlarge:
# https://aws.amazon.com/blogs/aws/new-storage-optimized-amazon-ec2-instances-i4i-powered-by-intel-xeon-scalable-ice-lake-processors/
# https://runs-on.com/benchmarks/ec2-storage/ (fio, ~4.0/3.1 GiB/s read/write)
SENDER_READ_MIBPS = 2.9e9 / 2**20
RECEIVER_READ_MIBPS = 4.0 * 1024
RECEIVER_WRITE_MIBPS = 3.1 * 1024

# Burst network bandwidth is best effort and depends on remaining network credits.
# The 10-minute and 30-second cases are examples, not promised durations.
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html
BURST_MIN = 10.0
SHORT_BURST_S = 30.0

# Page cache: Netflix's EC2 tuning, from Brendan Gregg, "How Netflix Tunes EC2
# Instances for Performance", AWS re:Invent 2017 (CMP325), slide 31,
# "5. File system": vm.dirty_ratio = 80 (from 40), vm.dirty_background_ratio
# = 5 (from 10), vm.dirty_expire_centisecs = 12000.
#   https://www.brendangregg.com/Slides/AWSreInvent2017_performance_tuning_EC2.pdf
# Writers run freely until dirty pages reach DIRTY_RATIO, then at the disk
# rate. The flusher writes back at the disk rate once dirty pages pass
# DIRTY_BACKGROUND_RATIO, a few seconds into a copy. Both ratios apply to
# available memory (free plus reclaimable), not all of RAM, so the model's
# limits are upper bounds. Near the limit the kernel holds writers close to
# the disk rate; the model lets them refill lost headroom with a time
# constant of DIRTY_REFILL_S, so a brief slowdown does not reopen the cache to
# the full wire rate. It leaves out the kernel's throttling further below the
# limit and the 120 s expiry writeback below the background threshold.
DIRTY_RATIO = 0.80
DIRTY_BACKGROUND_RATIO = 0.05
DIRTY_REFILL_S = 1.0             # assumed
# Exoshuffle-CloudSort measured 2.2 GB/s sequential writes with fio on one
# i4i.4xlarge local SSD: 2,098 MiB/s. Assume exact linear scaling by instance
# size: one quarter gives 524.5 MiB/s on xlarge, rounded to 525.
# https://arxiv.org/html/2301.03734v1#S3.SS1
NVME_WRITE_XLARGE = 525.0        # MiB/s; projected, not measured on xlarge
# The same fio run measured 2.9 GB/s reads on 4xlarge. Apply the same
# instance-size scaling to xlarge sender and verification reads, separately
# from write speed.
NVME_READ_XLARGE = 2.9e9 / 2**20 / 4
# A chosen gp3 provision, not the service's maximum. Baseline is published:
#   https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html
GP3_PROVISIONED_MIBPS = 1000.0
GP3_BASELINE_MIBPS = 125.0
# i7i.4xlarge has an EBS interface separate from its VPC network bandwidth.
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-optimized.html
EBS_4XLARGE_BASELINE = gbps_to_mibps(5)
EBS_4XLARGE_BURST = gbps_to_mibps(10)

# Codec CPU rates. Every rate below is per vCPU on the i4i's Intel Xeon
# Platinum 8375C (Ice Lake, 3.5 GHz all-core turbo), derated from tx's own
# codecs measured on one core of a desktop CPU.
#
# Measured: tx's SEND frame path (encoding.WrapCompressedWriter, fast mode,
# a new encoder per 4 MiB frame) and its decoders (WrapDecompressedReader)
# over the 202 MiB Silesia corpus, GOMAXPROCS=1, on an AMD Ryzen AI 9 HX 370
# (Zen 5, up to 5.16 GHz). klauspost/compress v1.18.4 zstd at level 1 and
# pierrec/lz4 v4.1.25 at Fast. The slowest of three runs, each the best of
# three passes:
#   zstd -1  ratio 2.860   289 MiB/s compress     858 MiB/s decompress
#   lz4      ratio 2.224   375 MiB/s compress   3,868 MiB/s decompress
# http://sun.aei.polsl.pl/~sdeor/index.php?page=silesia
C_MIBPS = {"lz4": 375.0, "zstd": 289.0}
C_DECODE_MIBPS = {"lz4": 3868.0, "zstd": 858.0}
# Clock: 3.5 GHz all-core turbo against the desktop core's 5.16 GHz. Zen 5's
# per-clock gain over Ice Lake is ignored, which favors the i4i.
CLOCK_FACTOR = 3.5 / 5.16        # assumed
# A vCPU is one hyperthread. Two busy hyperthreads get about 1.2 times one
# thread's throughput from their core, so each gets 0.6 of a core. The model
# assumes every sibling is busy, as it is on a loaded sender.
SMT_FACTOR = 0.6                 # assumed


def per_vcpu(mibps):
    """A measured desktop-core MiB/s, derated to MiB/s per i4i vCPU."""
    return mibps * CLOCK_FACTOR * SMT_FACTOR


# Logical MiB/s per vCPU: zstd 118, lz4 153 to compress; zstd 349, lz4 1,574
# to decompress.
COMPRESS_MIBPS = {m: per_vcpu(C_MIBPS[m]) for m in C_MIBPS}
DECODE_MIBPS = {m: per_vcpu(C_DECODE_MIBPS[m]) for m in C_MIBPS}

# Compression shares the sender's vCPUs with reading, xxh3 hashing,
# framing, the kernel's TCP send, and AES-GCM; decompression shares the
# receiver's with the same pipeline. There is no fixed split: fast mode
# compresses with every core (codec concurrency 0 in codec.go), and each of
# the many data connections compresses its own frames, so a mode's CPU limit
# is the vCPUs over its cost per logical MiB, pipeline plus codec. On
# i4i.4xlarge (16 vCPUs, 360 MiB/s per vCPU of pipeline) that is 1,715 MiB/s
# for lz4 and 1,418 MiB/s for zstd; on i4i.xlarge, a quarter of each.


def cpu_limit(vcpus, codec_mibps):
    """Logical MiB/s that vcpus sustain for the pipeline plus a codec."""
    cost = 1 / (TX_AES_MIBPS_PER_VCPU * TX_PARALLEL_EFFICIENCY)
    if codec_mibps:
        cost += 1 / codec_mibps
    return vcpus / cost


# Ratios describe bytes already on disk, before tx compression. All scenarios
# use 512 MiB files and the same codec CPU rates to isolate wire savings.
# Database segments are already lz4-compressed page by page: another lz4 pass
# gains 1.02:1; zstd's assumed 2:1 comes from redundancy across pages.
# Plain compressible files use the measured Silesia ratios quoted above.
INCOMPRESSIBLE = dict(ratio={"none": 1.0, "lz4": 1.0, "zstd": 1.0}, speedup=1.0)
LZ4_COMPRESSED = dict(ratio={"none": 1.0, "lz4": 1.02, "zstd": 2.0}, speedup=1.0)
COMPRESSIBLE = dict(ratio={"none": 1.0, "lz4": 2.224, "zstd": 2.860}, speedup=1.0)
LADDER = ("none", "lz4", "zstd")
CODEC_LABEL = {"none": "none", "lz4": "lz4", "zstd": "zstd -1"}

# The model approximates comparison margins, backoff, validation delays,
# and bounded real-frame checks of cheaper codecs. Candidate rates come
# directly from the hardware model; it does not replay Go's estimator or
# validation failures. Workers share spare sender CPU, with separate slots
# for results waiting for a frame to end.
FRAME_MIB = 4.0
WIN_FRAMES = 4                   # independent background samples to win
LOSS_FRAMES = 2                  # ... and loses after at least 2
TRIAL_CAP = 16                   # a near-tie runs to the cap
WIN_MARGIN = 0.03                # going up: beat home by 3%; down: within 3%
CAP_MARGIN = 0.06                # at the cap, the means decide with 6% margins
LOW_GAIN_RATIO = 1.10            # an upward trial compressing less ends after a frame
CHAIN_FRAMES = 8                 # zstd comparison after a losing lz4 sample
SAMPLE_MIB = .25                 # 256 KiB; both codecs, independent of lz4 gain
SAMPLE_EVERY = 8
PROBE_GAP = (24, 256)            # frames between a pair's probes: first, doubling cap
LOSS_GAP_PER_FRAME = 64          # a lost probe waits this many frames per frame lost
LOSS_GAP_CAP = 4096              # the cap once a loss costs a frame or the gap passed 256
REGIME_SHIFT = 1.2               # home's cost moves this much either way ...
REGIME_RATIO = 1.5               # ... or home's ratio moves this much ...
REGIME_FRAMES = 3                # ... for this many frames in a row: probe now


def sample_workers(vcpus):
    """Concurrent codec workers, assuming GOMAXPROCS equals sender vCPUs."""
    return max(1, vcpus // 8)


def sample_slots(vcpus):
    """Captured, queued, and completed samples awaiting consumption."""
    return max(2, vcpus // 4)


# The copied files' size: the database's segment files, about 512 MiB each.
# Change it and regenerate: it sets how much of each file a fresh policy
# sends uncompressed.
FILE_MIB = 512.0

# Compression evidence is per file: a file starts at none and carries its
# state across its own windows. The link is shared: a file with no state of
# its own seeds its uncompressed baseline from its connection's, so it needs
# only its own winning samples. The model keeps one state to decide the codec
# home settles on, steps all connections in lockstep (a "frame" below is one
# frame on every connection), and charges each file the frames it sends
# before its own policy switches: SWITCH_FRAMES independent winning samples.
# Both sampled codecs can be compared from the first result. (A
# connection's first file also seeds its own baseline, 3 frames more; the
# model ignores that one-time cost.) A file's windows are not sequential,
# though. The client keeps several of a file's windows in flight, and each new
# window loads the state the last finished one saved, so the state advances
# in interleaved chains, one per window in flight. client_plan mirrors the
# client's fast-mode plan (buildBatchSizePlan in client.go) to find each
# chain's length.
SWITCH_FRAMES = 4
TARGET_IO_DEPTH = 4              # server io-depth per vCPU (DefaultTargetIODepth)
WINDOW_FILES = 4                 # files in flight (effectiveWindowConcurrency)
REQUEST_WINDOW_MIB = 512         # defaultClientRequestWindowBytes
SOCKET_FLOOR_MIB = 4             # rounded socket buffer floor on batches


def pow2_up(mib):
    """roundUpPow2MiB, in MiB."""
    return 2 ** int(np.ceil(np.log2(max(1.0, mib))))


def pow2_within(mib):
    """largestPow2MiBWithin, in MiB, for at least 1 MiB."""
    return 2 ** int(np.floor(np.log2(mib)))


def client_plan(sender_vcpus, link_gbps, file_mib=FILE_MIB):
    """The client's fast-mode plan for files of file_mib.

    Returns the data connections, the window size, the windows of a file in
    flight, and plain_share: the share of each file a fresh policy sends
    before its chain of windows reaches SWITCH_FRAMES frames. link_gbps is
    the probe's link estimate, taken during the burst.
    """
    conns = sender_vcpus * TARGET_IO_DEPTH
    per_file = max(1, conns // WINDOW_FILES)
    batch = pow2_up(REQUEST_WINDOW_MIB / per_file)
    batch = min(batch, pow2_within(link_gbps * 1e9 / 8 / conns / 2 / 2**20))
    batch = min(max(batch, SOCKET_FLOOR_MIB), pow2_within(REQUEST_WINDOW_MIB))
    windows = max(1, int(np.ceil(file_mib / batch)))
    in_flight = min(per_file, REQUEST_WINDOW_MIB // batch, windows)
    chain_frames = np.ceil(windows / in_flight) * min(batch, file_mib) / FRAME_MIB
    return dict(conns=conns, batch_mib=batch, in_flight=in_flight,
                plain_share=min(1.0, SWITCH_FRAMES / chain_frames))


# Figure 1: copy 80% of i4i.4xlarge's SSD to an empty i7i.4xlarge, local NVMe.
F1_PLAN = client_plan(16, 25.0)
F1_CONNS = F1_PLAN["conns"]
F1_SECONDS = 50 * 60             # enough for all modeled strategies to finish
F1_BIN_S = 3.0                  # throughput moving average
F1_DATASET_GIB = LOCAL_4XLARGE_GIB * COPY_FRACTION

# Transfer table: per-vCPU rate of tx's non-codec pipeline.
TX_AES_MIBPS_PER_VCPU = 450.0    # assumed
TX_PARALLEL_EFFICIENCY = 0.80    # assumed

# Page-cache figure.
F4_MINUTES = 30
F4_HIT_LATENCY_US = 20.0         # hit: memory plus syscall path, median; assumed
F4_MISS_LATENCY_US = 250.0       # miss: NVMe/EBS read, median (0.1-1 ms range); assumed
F4_SIGMA_HIT = 0.4               # lognormal spread; assumed
F4_SIGMA_MISS = 0.6              # assumed
F4_HIT_STEADY = 0.999            # steady-state hit ratio; assumed
F4_HIT_COLD = 0.0                # empty cache after cutover
F4_HIT_RESTORED = 0.995          # pages evicted or not yet restored; assumed
F4_WARM_TAU_MIN = 5.0            # demand-fill time constant of the hot set; assumed

# Verification: uncached reads on an otherwise idle i7i.4xlarge. CPU, IOPS,
# sample seeks, metadata, and concurrent writeback can lower actual coverage.
VERIFY_SECONDS = 60.0
VERIFY_DATASET_GIB = F1_DATASET_GIB

# ---------------------------------------------------------------------------
# Shared style. Okabe-Ito colors; each strategy keeps one color, line style,
# and marker in every figure, so the figures read in grayscale too.
# ---------------------------------------------------------------------------

BLACK, ORANGE, GREEN = "#000000", "#E69F00", "#009E73"
BLUE, VERMILLION, PURPLE = "#0072B2", "#D55E00", "#CC79A7"
INK, MUTED, REF = "#1a1a1a", "#555555", "#8c8c8c"
FIG_W = 7.0                       # double-column width, inches
NOTE = "Illustrative model; not measurements."

STRATEGY = {
    "tx":     dict(color=BLUE, lw=2.8, ls="-", zorder=3),
    "none":   dict(color=ORANGE, lw=1.6, ls=(0, (5, 2.5)), zorder=4),
    "lz4":    dict(color=PURPLE, lw=1.6, ls=(0, (2, 1.5)), zorder=5),
    "zstd":   dict(color=GREEN, lw=1.6, ls=(0, (7, 2, 1.5, 2)), zorder=5),
    "inline": dict(color=VERMILLION, lw=1.8, ls=(0, (1.2, 1.6)), zorder=4),
    "rsync":  dict(color=PURPLE, lw=1.6, ls=(0, (5, 2.5)), marker="^", ms=7, zorder=4),
    "rsync_ssh": dict(color=VERMILLION, lw=1.6, ls=(0, (7, 2, 1.5, 2)), marker="D",
                      ms=6, zorder=4),
    "cold":   dict(color=BLACK, lw=1.6, ls=(0, (5, 2.5)), zorder=4),
    # The optimal rate: thin and on top, so tx is seen to sit on it.
    "optimal": dict(color="#3d3d3d", lw=1.0, ls=(0, (3, 2)), zorder=6),
}
REFLINE = {
    "disk": dict(color=REF, lw=1.0, ls=(0, (2, 2)), zorder=2),
    "network_burst": dict(color=MUTED, lw=1.8, ls="-", zorder=2),
    "network_baseline": dict(color=REF, lw=1.2, ls=(0, (5, 3)), zorder=2),
}


def style():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 9,
        "axes.labelsize": 9.5, "axes.titlesize": 9.5, "axes.titleweight": "bold",
        "axes.titlelocation": "left", "axes.titlepad": 4,
        "xtick.labelsize": 9, "ytick.labelsize": 9, "legend.fontsize": 9,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.linewidth": .8, "axes.edgecolor": INK, "axes.labelcolor": INK,
        "text.color": INK, "xtick.color": INK, "ytick.color": INK,
        "xtick.major.width": .8, "ytick.major.width": .8,
        "xtick.major.size": 3, "ytick.major.size": 3,
        "xtick.minor.size": 1.8, "ytick.minor.size": 1.8,
        "axes.grid": True, "axes.grid.axis": "y", "grid.color": "#e6e6e6",
        "grid.linewidth": .6, "axes.axisbelow": True,
        "figure.facecolor": "white", "axes.facecolor": "white",
        "savefig.facecolor": "white", "legend.frameon": False,
        "legend.handlelength": 2.6, "lines.solid_capstyle": "butt",
        "lines.dash_capstyle": "butt",
    })
    return plt


def new_figure(height, width=FIG_W):
    plt = style()
    fig = plt.figure(figsize=(width, height), layout="constrained")
    fig.get_layout_engine().set(w_pad=2 / 72, h_pad=2 / 72, wspace=.02, hspace=.02)
    fig.supxlabel(NOTE, x=.995, ha="right", fontsize=9, color=MUTED, fontstyle="italic")
    return fig


def thousands(ax, axis="y"):
    from matplotlib.ticker import FuncFormatter
    f = FuncFormatter(lambda v, _: f"{v:,.0f}")
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(f)


def line(ax, x, y, key, **kw):
    return ax.plot(x, y, **{**STRATEGY[key], **kw})


def label(ax, x, y, text, color=INK, **kw):
    kw.setdefault("fontsize", 9)
    t = ax.text(x, y, text, color=color, **kw)
    t.set_in_layout(False)
    return t


def arrow(ax, text, xy, xytext, color=INK, **kw):
    kw.setdefault("fontsize", 9)
    a = ax.annotate(text, xy=xy, xytext=xytext, color=color,
                    arrowprops=dict(arrowstyle="-|>", lw=.7, color=color,
                                    shrinkA=1, shrinkB=2, mutation_scale=7), **kw)
    a.set_in_layout(False)
    return a


def save(fig, name):
    PLOTS.mkdir(exist_ok=True)
    png = PLOTS / f"{name}.png"
    fig.savefig(png, dpi=300, metadata={"Software": None})
    import matplotlib.pyplot as plt
    plt.close(fig)
    return (png,)


# ---------------------------------------------------------------------------
# Model.
# ---------------------------------------------------------------------------


def offered_rates(wire, data, vcpus, decode_vcpus=None):
    """Logical MiB/s each mode can sustain before the receiver's disk.

    Each mode is the slowest of the wire times its ratio and the CPU limits
    of the sender's vcpus and the receiver's decode_vcpus (the sender's
    count by default), each running the pipeline plus that mode's codec.
    """
    out = {}
    if decode_vcpus is None:
        decode_vcpus = vcpus
    sp = data["speedup"]
    for m in LADDER:
        encode = 0 if m == "none" else COMPRESS_MIBPS[m] * sp
        decode = 0 if m == "none" else DECODE_MIBPS[m] * sp
        out[m] = min(wire * data["ratio"][m], cpu_limit(vcpus, encode),
                     cpu_limit(decode_vcpus, decode))
    return out


def new_policy():
    """State for throughput_first: an adaptive file starts at none."""
    pairs = [dict(gap=PROBE_GAP[0], wait=PROBE_GAP[0], lost=False) for _ in LADDER[1:]]
    return dict(home="none", home_rate=None, ref=None, ref_ratio=None, streak=0,
                trial=None, pairs=pairs)


def reset_pair(p):
    """A win or a regime change restarts a pair's probe schedule."""
    p.update(gap=PROBE_GAP[0], wait=PROBE_GAP[0], lost=False)


def lose_gap(p, lost):
    """Compound a pair's probe gap after a lost probe that cost `lost` frames.

    The first loss since a reset waits PROBE_GAP[0] frames, and each later one
    twice the previous gap; either way at least LOSS_GAP_PER_FRAME frames per
    frame lost. The cap is PROBE_GAP[1], rising to LOSS_GAP_CAP once a loss
    costs a frame or more or the gap already passed PROBE_GAP[1]. The gap
    never shrinks between resets.
    """
    prev = p["gap"]
    gap = 2 * prev if p["lost"] else PROBE_GAP[0]
    gap = max(gap, LOSS_GAP_PER_FRAME * lost)
    cap = LOSS_GAP_CAP if lost >= 1 or prev > PROBE_GAP[1] else PROBE_GAP[1]
    p.update(gap=max(prev, min(gap, cap)), lost=True)
    p["wait"] = p["gap"]


def start_trial(s, mode, cap=TRIAL_CAP, back=False, lost=0.0):
    """Begin a trial of `mode` against home; pair i joins rungs i and i+1."""
    rung = LADDER.index
    s["trial"] = dict(mode=mode, n=0, total=0.0, cap=cap, back=back, lost=lost,
                      pair=min(rung(mode), rung(s["home"])))
    return mode


def throughput_first(s, mode, rate, ratio):
    """Advance the comparison engine; candidates never become wire trials.

    s is the state from new_policy(). mode is an observed home codec or a
    background candidate, rate its estimated logical rate, and ratio its
    compression ratio (for none,
    the lz4 ratio of the policy's sniff). Each pair of neighboring rungs
    schedules its own probes and compounds its gap over repeated losses
    (lose_gap). A shift of home's cost or ratio for REGIME_FRAMES frames
    resets both neighbors' gaps and probes them at once. A win moves home,
    resets the pair, and checks the way back at once.
    """
    rung = LADDER.index
    t = s["trial"]
    if t is not None:
        t["n"] += 1
        t["total"] += rate
        mean, home = t["total"] / t["n"], s["home_rate"]
        r = mean / home
        up = rung(t["mode"]) > rung(s["home"])
        near = abs(r - 1) < CAP_MARGIN
        margin = CAP_MARGIN if near else WIN_MARGIN
        won = r >= 1 + margin if up else r >= 1 - margin
        if up and t["n"] == 1 and ratio < LOW_GAIN_RATIO:
            won = False
        elif t["n"] < t["cap"] and (near or t["n"] < (WIN_FRAMES if won else LOSS_FRAMES)):
            return t["mode"]
        lost = t["lost"] + t["n"] * max(0.0, home / mean - 1)
        if not won and s["home"] == "none" and t["mode"] == "lz4":
            return start_trial(s, "zstd", CHAIN_FRAMES, lost=lost)
        s["trial"] = None
        p = s["pairs"][t["pair"]]
        if not won:
            lose_gap(p, lost)
            return s["home"]
        reset_pair(p)
        old = s["home"]
        s.update(home=t["mode"], home_rate=mean, ref=None, streak=0)
        if t["back"]:
            return s["home"]
        toward = LADDER[rung(s["home"]) + (1 if rung(old) > rung(s["home"]) else -1)]
        return start_trial(s, toward, back=True)

    s["home_rate"] = rate
    if s["ref"] is None:
        s["ref"], s["ref_ratio"] = rate, ratio
    moved = (max(rate / s["ref"], s["ref"] / rate) >= REGIME_SHIFT or
             max(ratio / s["ref_ratio"], s["ref_ratio"] / ratio) >= REGIME_RATIO)
    s["streak"] = s["streak"] + 1 if moved else 0
    h = rung(s["home"])
    near = [i for i in (h, h - 1) if 0 <= i < len(s["pairs"])]   # up pair first
    if s["streak"] >= REGIME_FRAMES:
        s["ref"], s["ref_ratio"], s["streak"] = rate, ratio, 0
        for i in near:
            reset_pair(s["pairs"][i])
            s["pairs"][i]["wait"] = 0
    for i in near:
        s["pairs"][i]["wait"] -= 1
    for i in near:
        if s["pairs"][i]["wait"] <= 0:
            return start_trial(s, LADDER[i + 1] if i == h else LADDER[i])
    return s["home"]


def dirty_limits(size):
    """Background and dirty thresholds in MiB for an i4i size."""
    ram = I4I[size]["ram"] * GIB
    return DIRTY_BACKGROUND_RATIO * ram, DIRTY_RATIO * ram


def simulate(strategy, wire, disk, cache, vcpus, conns, seconds, data=LZ4_COMPRESSED,
             total_mib=float("inf"), sender_read_mibps=float("inf"),
             decode_vcpus=None, plain_share=0.0,
             window_mib=float("inf")):
    """Step a copy one frame per connection at a time.

    wire(t) is the network rate in MiB/s on the wire. cache is the
    receiver's (background, limit) in MiB from dirty_limits: the disk drains
    dirty pages at its rate once they pass the background threshold, and the
    receiver accepts at most the disk rate plus the headroom left below the
    limit per DIRTY_REFILL_S, so the copy settles at the disk rate. A static
    strategy keeps its mode; "tx" sends home while throughput_first compares
    background estimates; "optimal" is an
    oracle that sends every frame in the fastest mode for that instant, with
    no trials or lag. The oracle is the fastest at every instant, but the
    rate is path-dependent: a strategy that was CPU-bound early still has
    page cache to fill and can later run above it. Returns a dict with frame
    start and end times, mode and logical rate per frame, and the completion
    time in seconds (None if the copy outlasts `seconds`). sender_read_mibps
    bounds logical bytes independently of wire savings. vcpus and
    decode_vcpus are the sender's and receiver's vCPUs (offered_rates).
    For tx, plain_share of each file goes uncompressed while the file's own
    policy catches up (client_plan), blended into the logical rate; a file
    that never catches up sends none. Sampling uses a fair cohort of streams,
    not the client's actual window scheduler. window_mib bounds the time a
    sample has before its input window ends.
    """
    background, dirty_limit = cache
    tx = strategy == "tx"
    workers, slots = sample_workers(vcpus), sample_slots(vcpus)
    pol = new_policy() if tx else None
    mode = "none" if tx else strategy
    t = dirty = copied = 0.0
    done = None
    t0s, t1s, modes, homes, rates, sample_cpus = [], [], [], [], [], []
    sample_job = None
    check = None
    recovery_debt = 0.0
    validate_left = 0
    frames = 0
    while t < seconds and done is None:
        offered = offered_rates(wire(t), data, vcpus, decode_vcpus)
        accept = disk + (dirty_limit - dirty) / DIRTY_REFILL_S
        true = {m: min(r, accept, sender_read_mibps)
                for m, r in offered.items()}
        if strategy == "optimal":
            mode = max(LADDER, key=true.get)
        if check is not None:
            mode = check["mode"]
        rate = true[mode]  # what home's frames show the policy
        sent = mode
        logical = rate
        if tx and mode != "none" and check is None:
            if plain_share >= 1:
                sent, logical = "none", true["none"]
            elif plain_share > 0:
                logical = 1 / (plain_share / true["none"] + (1 - plain_share) / rate)
        dt = FRAME_MIB * conns / logical
        if copied + logical * dt >= total_mib:
            dt = (total_mib - copied) / logical
            done = t + dt
        fill = dirty + logical * dt
        if dirty > background:
            fill = max(background, fill - disk * dt)
        dirty = min(dirty_limit, fill)
        copied += logical * dt
        t0s.append(t)
        t1s.append(t + dt)
        modes.append(sent)
        homes.append(pol["home"] if tx else mode)
        rates.append(logical)
        cpu = 0.0
        if sample_job is not None:
            active = max(0.0, min(t + dt, sample_job["ready"]) - t)
            cpu = sample_job["cpu"] * active / dt
        sample_cpus.append(cpu)
        if tx:
            frames += 1
            if check is not None:
                loss = max(0, dt - FRAME_MIB * conns / check["rate"])
                recovery_debt = recovery_debt + 100 * loss if loss else max(0, recovery_debt - dt)
                if logical > 1.03 * check["rate"]:
                    check["n"] += 1
                    if check["n"] == 2:
                        pol.update(home=mode, home_rate=rate, ref=None, streak=0, trial=None)
                        for pair in pol["pairs"]:
                            pair["wait"] = PROBE_GAP[1]
                        check = None
                        validate_left = 8
                elif mode == "none" and pol["home"] == "zstd":
                    check.update(mode="lz4", n=0)
                else:
                    check = None
                mode = pol["home"]
                t += dt
                continue
            recovery_debt = max(0, recovery_debt - dt)
            if validate_left:
                validate_left -= 1
                t += dt
                continue
            old_home = pol["home"]
            sniff = data["ratio"]["lz4" if mode == "none" else mode]
            if pol["trial"] is None:
                throughput_first(pol, mode, rate, sniff)
            else:
                pol["home_rate"] = rate  # Real home observations continue.
            remaining = total_mib - copied
            evidence = max(1, WIN_FRAMES - (pol["trial"]["n"] if pol["trial"] else 0))
            needed = evidence + 1 + max(0, 4 - frames)
            useful = remaining >= needed * FRAME_MIB * conns
            if pol["trial"] is not None and pol["home"] == "none" and pol["trial"]["n"] == 0 and true["zstd"] > 1.03 * true["lz4"]:
                pol["trial"].update(mode="zstd", cap=CHAIN_FRAMES)
            if sample_job is not None and sample_job["ready"] <= t + dt:
                if pol["trial"] is not None:
                    target = pol["trial"]["mode"]
                    throughput_first(pol, target, true[target], data["ratio"][target])
                sample_job = None
            if pol["home"] != old_home:
                validate_left = 8
                mode = pol["home"]
                t += dt
                continue
            pending = pol["trial"] is not None
            if pending and LADDER.index(pol["trial"]["mode"]) < LADDER.index(pol["home"]):
                if recovery_debt == 0 and FILE_MIB >= 20 and remaining >= 20 * conns:
                    check = dict(mode="none", n=0, rate=logical)
                    sample_job = None
                pol["trial"] = None
                for pair in pol["pairs"]:
                    pair["wait"] = max(pair["wait"], PROBE_GAP[0])
                pending = False
            if sample_job is None and useful and frames >= 4 and (pending or frames % SAMPLE_EVERY == 0):
                per_job = SAMPLE_MIB * sum(1 / COMPRESS_MIBPS[c] for c in ("lz4", "zstd")) / data["speedup"]
                work = conns * per_job
                codec_cpu = 0 if mode == "none" else rate / (COMPRESS_MIBPS[mode] * data["speedup"])
                pipeline_cpu = rate / (TX_AES_MIBPS_PER_VCPU * TX_PARALLEL_EFFICIENCY)
                spare = max(.01, vcpus - codec_cpu - pipeline_cpu)
                busy = min(workers, spare)
                # Workers release CPU when encoding finishes. Slots remain
                # occupied until a frame consumes the result. Fair turns
                # across all streams approximate that separate constraint.
                latency = max(.001, min(conns, slots) * per_job / busy)
                delay = max(work / busy, dt * np.ceil(conns / slots))
                window_time = window_mib * conns / rate
                if remaining / rate > delay * needed and window_time > 4 * latency:
                    sample_job = dict(ready=t + dt + delay, cpu=work / delay)
            mode = pol["home"]
        t += dt
    return dict(t0=np.array(t0s), t1=np.array(t1s), mode=modes, home=homes, rate=np.array(rates),
                done=done, data=data, sample_cpu=np.array(sample_cpus))


def rate_series(res, scale=1.0, bin_s=None):
    """Logical rate as steps-post points, one per run of equal rate.

    With bin_s, the rate is a centered moving average over bin_s seconds, as
    a throughput monitor would show it: a few trial frames cost their
    share of the window instead of drawing a full-height spike, and a
    regime change stays centered on its true time.
    """
    if bin_s is None:
        r = res["rate"]
        starts = np.concatenate([[0], np.flatnonzero(np.diff(r)) + 1])
        xs = np.append(res["t0"][starts], res["t1"][-1]) / scale
        return xs, np.append(r[starts], r[starts][-1])
    end = res["t1"][-1]
    t = np.append(0, res["t1"])
    cum = np.append(0, np.cumsum(res["rate"] * (res["t1"] - res["t0"])))
    grid = np.append(np.arange(0, end, bin_s / 10), end)
    lo, hi = np.maximum(grid - bin_s / 2, 0), np.minimum(grid + bin_s / 2, end)
    avg = (np.interp(hi, t, cum) - np.interp(lo, t, cum)) / (hi - lo)
    # Merge neighboring points within 1% of their run's mean, so the phase of
    # trials against the window does not draw a ripple.
    xs, ys, run = [grid[0]], [], [avg[0]]
    for g, v in zip(grid[1:-1], avg[1:-1]):
        m = sum(run) / len(run)
        if abs(v - m) > .01 * m:
            ys.append(m)
            xs.append(g)
            run = []
        run.append(v)
    ys.append(sum(run) / len(run))
    return np.append(xs, end) / scale, np.append(ys, ys[-1])


def cum_at(res, t):
    """MiB moved by time t (seconds)."""
    cum = np.append(0, np.cumsum(res["rate"] * (res["t1"] - res["t0"])))
    return float(np.interp(t, np.append(0, res["t1"]), cum))


def first_drop(res):
    """Time the logical rate first falls below its starting value."""
    r = res["rate"]
    return res["t0"][int(np.argmax(r < r[0] - 1))]


def runs(res):
    """Merge consecutive frames in one mode into (start, end, mode) spans."""
    out = []
    for t0, t1, m in zip(res["t0"], res["t1"], res["mode"]):
        if out and out[-1][2] == m:
            out[-1][1] = t1
        else:
            out.append([t0, t1, m])
    return out


def vcpus_used(res):
    """Sender vCPUs compressing in each frame."""
    sp = res["data"]["speedup"]
    used = np.array([0.0 if m == "none" else r / (COMPRESS_MIBPS[m] * sp)
                     for r, m in zip(res["rate"], res["mode"])])
    return used + res.get("sample_cpu", 0)


def break_marks(left, right):
    """Diagonal marks where a broken x axis jumps from `left` to `right`."""
    kw = dict(marker=[(-1, -2), (1, 2)], markersize=7, linestyle="none", color=INK,
              mec=INK, mew=.8, clip_on=False)
    left.plot([1], [0], transform=left.transAxes, **kw)
    right.plot([0], [0], transform=right.transAxes, **kw)


# ---------------------------------------------------------------------------
# Figure 1: burst networking runs out mid-copy.
# ---------------------------------------------------------------------------


def figure_burst():
    from cloud_transfer_figures import figure_burst as render
    return render()


# ---------------------------------------------------------------------------
# The page cache has to travel too.
# ---------------------------------------------------------------------------


def p99_us(hit):
    """99th percentile of a hit/miss lognormal mixture, by bisection."""
    nd = NormalDist()

    def cdf(x, med, sigma):
        return nd.cdf((np.log(x) - np.log(med)) / sigma)

    res = []
    for h in hit:
        lo, hi = 1.0, 1e6
        for _ in range(60):
            mid = np.sqrt(lo * hi)
            f = h * cdf(mid, F4_HIT_LATENCY_US, F4_SIGMA_HIT) + \
                (1 - h) * cdf(mid, F4_MISS_LATENCY_US, F4_SIGMA_MISS)
            lo, hi = (mid, hi) if f < .99 else (lo, mid)
        res.append(np.sqrt(lo * hi))
    return np.array(res)


def latency_label(v, _):
    return f"{v / 1000:g} ms" if v >= 1000 else f"{v:g} µs"


def figure_page_cache(width=FIG_W, name="cloud-page-cache"):
    from matplotlib.lines import Line2D
    from matplotlib.ticker import FixedLocator, FuncFormatter, LogLocator, NullFormatter
    t = np.arange(0, F4_MINUTES * 60 + 1, 5.0) / 60

    def hit(start):
        return F4_HIT_STEADY - (F4_HIT_STEADY - start) * np.exp(-t / F4_WARM_TAU_MIN)

    cold, warm = hit(F4_HIT_COLD), hit(F4_HIT_RESTORED)
    cross = F4_WARM_TAU_MIN * np.log((F4_HIT_STEADY - F4_HIT_COLD) / (F4_HIT_STEADY - .99))
    narrow = width < FIG_W
    fig = new_figure(4.3 if not narrow else 4.0, width)
    gs = fig.add_gridspec(2, 1, height_ratios=[1.3, 1])
    ax = fig.add_subplot(gs[0])
    bx = fig.add_subplot(gs[1], sharex=ax)

    line(ax, t, p99_us(cold), "cold")
    line(ax, t, p99_us(warm), "tx")
    ax.set_yscale("log")
    ax.set_ylim(10, 3000)
    ax.yaxis.set_major_locator(FixedLocator([10, 100, 1000]))
    ax.yaxis.set_major_formatter(FuncFormatter(latency_label))
    ax.yaxis.set_minor_locator(LogLocator(base=10, subs=np.arange(2, 10)))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_ylabel("p99 read latency")
    ax.set_title("(a) p99 read latency after cutover")
    ax.tick_params(labelbottom=False)
    for v, txt in ((F4_MISS_LATENCY_US, "disk read, median"), (F4_HIT_LATENCY_US, "page-cache hit, median")):
        ax.axhline(v, **REFLINE["disk"])
        if narrow:
            label(ax, .4, v * 1.12, txt, ha="left", va="bottom", color=MUTED)
        else:
            label(ax, F4_MINUTES, v * 1.12, txt, ha="right", va="bottom", color=MUTED)

    bx.plot(t, 100 * cold, **STRATEGY["cold"])
    bx.plot(t, 100 * warm, **STRATEGY["tx"])
    bx.axhline(99, **REFLINE["disk"])
    bx.set_ylim(0, 108)
    bx.set_yticks([0, 25, 50, 75, 100])
    bx.set_ylabel("Hit ratio (%)")
    bx.set_xlabel("Time since cutover (min)")
    bx.set_xlim(0, F4_MINUTES)
    bx.set_title("(b) Page-cache hit ratio")
    for a in (ax, bx):
        a.axvline(cross, color=MUTED, lw=.7, zorder=1)
    if not narrow:
        label(ax, .4, 1300, "cold start: first touches read from disk", va="center")
        label(ax, .4, 70, "tx --cache-load: restored before cutover", va="center")
        arrow(bx, f"cold start passes\n99% hits at {cross:.0f} min", xy=(cross - .1, 98.5),
              xytext=(cross - 1.2, 62), ha="right", va="center")
        label(bx, 8.5, 22, f"cold start warms with an assumed\ntime constant of {F4_WARM_TAU_MIN:g} min",
              va="center")
        label(bx, .4, 96, f"tx: {F4_HIT_RESTORED:.1%} restored", va="top")
    else:
        handles = [Line2D([], [], **STRATEGY["tx"], label="tx --cache-load"),
                   Line2D([], [], **STRATEGY["cold"], label="cold start")]
        fig.legend(handles=handles, loc="outside upper center", ncol=2)
    return save(fig, name)


def verification_limits():
    """Matched-range disk-read ceilings: the slower of sender and receiver."""
    rates = [("Local NVMe · Sender limited", RECEIVER_READ_MIBPS),
             ("gp3 · EBS burst", min(GP3_PROVISIONED_MIBPS, EBS_4XLARGE_BURST)),
             ("gp3 · EBS baseline", min(GP3_PROVISIONED_MIBPS, EBS_4XLARGE_BASELINE)),
             ("gp3 · volume baseline", GP3_BASELINE_MIBPS)]
    return [(name, min(SENDER_READ_MIBPS, rate)) for name, rate in rates]


def figure_verification():
    limits = verification_limits()
    coverage = [min(VERIFY_DATASET_GIB, rate * VERIFY_SECONDS / GIB) for _, rate in limits]
    assert all(a > b > 0 for a, b in zip(coverage, coverage[1:]))
    assert max(coverage) < VERIFY_DATASET_GIB
    fig = new_figure(3.0)
    ax = fig.add_subplot()
    ys = np.arange(len(limits))
    ax.barh(ys, coverage, height=.5, color=[BLUE, GREEN, ORANGE, VERMILLION], zorder=3)
    ax.set_yticks(ys, [name for name, _ in limits])
    ax.invert_yaxis()
    ax.set_xlim(0, 240)
    ax.set_xticks([0, 60, 120, 180, 240])
    ax.grid(False, axis="y")
    ax.grid(True, axis="x")
    ax.set_xlabel(f"Data reread in {VERIFY_SECONDS:g} seconds (GiB)")
    ax.set_title(f"{VERIFY_SECONDS:g}-second verification I/O ceilings · {VERIFY_DATASET_GIB:,.0f} GiB copy")
    for y, ((name, rate), gib) in enumerate(zip(limits, coverage)):
        pct = 100 * gib / VERIFY_DATASET_GIB
        label(ax, gib + 3, y, f"{gib:.1f} GiB ({pct:.1f}%)", va="center", fontsize=8)
        print(f"Verify {name}: {rate:.0f} MiB/s, {gib:.1f} GiB ({pct:.1f}%) "
              f"in {VERIFY_SECONDS:g} s; full read {VERIFY_DATASET_GIB * GIB / rate / 60:.1f} min")
    return save(fig, "cloud-verification")


def plot():
    for f in (figure_burst, figure_page_cache, figure_verification):
        for p in f():
            print(p)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plot", action="store_true", help="render the figures into plots/")
    if ap.parse_args().plot:
        plot()
    else:
        ap.print_help()
