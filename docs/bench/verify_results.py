"""Recompute verification comparisons; --plot also regenerates the figure."""

import argparse
import csv
import math
from pathlib import Path
import random
import statistics
from collections import defaultdict

DEST = Path(__file__).resolve().parent
SIZES = ["4KiB", "16KiB", "64KiB", "1MiB", "4MiB", "16MiB"]
with (DEST / "data/verify-simple.csv").open() as source:
    rows = list(csv.DictReader(source))
for row in rows:
    for field in ("run", "cpus", "concurrency"):
        row[field] = int(row[field])
D = {(r["profile"], r["build"], r["run"], r["size"], r["cpus"], r["concurrency"]): r for r in rows}
if len(D) != len(rows):
    raise ValueError("duplicate benchmark observation")


def med(profile, build, size, cpus, concurrency, metric):
    return statistics.median(
        float(r[metric]) for r in rows
        if (r["profile"], r["build"], r["size"], r["cpus"], r["concurrency"])
        == (profile, build, size, cpus, concurrency)
    )


def compare(profile, build, base="sequential", metric="MiB/s", subset=lambda r: True):
    by_run = defaultdict(list)
    cases = defaultdict(set)
    for r in rows:
        if r["profile"] != profile or r["build"] != build or not subset(r):
            continue
        case = (r["size"], r["cpus"], r["concurrency"])
        baseline = D[(profile, base, r["run"], *case)]
        by_run[r["run"]].append(math.log(float(r[metric]) / float(baseline[metric])))
        cases[r["run"]].add(case)
    if set(by_run) != set(range(1, 7)) or any(v != cases[1] for v in cases.values()):
        raise ValueError("comparison requires six complete paired runs")
    means = [statistics.mean(v) for _, v in sorted(by_run.items())]
    rng = random.Random(1729)
    boot = sorted(statistics.mean(rng.choices(means, k=6)) for _ in range(10000))
    return tuple(100 * math.expm1(x) for x in (statistics.mean(means), boot[249], boot[9749]))


def plot():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import TwoSlopeNorm

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11,
                         "axes.spines.top": False, "axes.spines.right": False,
                         "axes.titleweight": "bold", "figure.facecolor": "#f6f8fb",
                         "axes.facecolor": "white", "savefig.facecolor": "#f6f8fb"})
    fig, axs = plt.subplots(2, 2, figsize=(16, 12), gridspec_kw={"height_ratios": [1.25, 1]})
    fig.subplots_adjust(top=.89, bottom=.11, hspace=.34, wspace=.28, left=.10, right=.96)
    fig.suptitle("Verification throughput: overlap local hashing with CXSUM", fontsize=21, fontweight="bold", y=.97)
    fig.text(.5, .927, "Six alternating runs/implementation · shared CPU budget · advertised I/O depth 4 · serial items per batch/request",
             ha="center", fontsize=11, color="#415168")
    labels = [(cpu, c) for cpu in [2, 4, 8, 12] for c in [1, 2 * cpu]]
    for ax, build, title in [(axs[0, 0], "overlap", "Overlap vs sequential (c workers)"),
                             (axs[0, 1], "optimized", "Optimized overlap vs sequential")]:
        values = [[100 * (med("cached", build, size, cpu, c, "MiB/s") /
                          med("cached", "sequential", size, cpu, c, "MiB/s") - 1)
                   for size in SIZES] for cpu, c in labels]
        im = ax.imshow(values, cmap="RdYlGn", norm=TwoSlopeNorm(vmin=-30, vcenter=0, vmax=100), aspect="auto")
        ax.set_xticks(range(6), SIZES)
        ax.set_yticks(range(8), [f"{cpu} cores · c={c}" for cpu, c in labels])
        ax.set_title(title, pad=13)
        for y, line in enumerate(values):
            for x, v in enumerate(line):
                ax.text(x, y, f"{v:+.0f}%", ha="center", va="center", fontsize=10,
                        color="white" if v > 70 or v < -22 else "#172637")
        ax.tick_params(length=0, pad=8)
        if build == "optimized":
            fig.colorbar(im, ax=ax, fraction=.034, pad=.025).set_label("Throughput change", fontsize=10)
        ax.set_xlabel("File size", labelpad=8)
    ax = axs[1, 0]
    for c, color in [(1, "#2376b7"), (16, "#d87525")]:
        for build, name, ls, marker in [("pipeline", "pipeline", "--", "o"),
                                        ("optimized", "optimized overlap", "-", "s")]:
            values = [med("pipeline", build, size, 8, c, "MiB/s") for size in SIZES]
            workers = 2 * c if build == "pipeline" else c
            ax.plot(range(6), values, color=color, linestyle=ls, marker=marker, linewidth=2,
                    label=f"c={c}: {name}, {workers} worker" + ("s" if workers != 1 else ""))
    ax.set_xticks(range(6), SIZES)
    ax.set_yscale("log")
    ax.set_ylabel("Throughput (MiB/s, log scale)")
    ax.set_title("Eight cores: batch pipeline vs optimized overlap", pad=13)
    ax.grid(axis="y", alpha=.2)
    ax.legend(fontsize=8, loc="lower right", framealpha=.95)
    ax.set_xlabel("File size")
    ax = axs[1, 1]
    settings = [(p, s) for p in ["gp3", "i4i"] for s in ["4KiB", "64KiB", "4MiB"]]
    for offset, build, color, label in [(-.18, "overlap", "#2376b7", "Overlap"),
                                       (.18, "optimized", "#25926b", "Optimized overlap")]:
        values, low, high = [], [], []
        for profile, size in settings:
            v, lo, hi = compare(profile, build, subset=lambda r: r["size"] == size)
            values.append(1 + v / 100)
            low.append((v - lo) / 100)
            high.append((hi - v) / 100)
        ax.bar([i + offset for i in range(6)], values, width=.34, color=color,
               label=label, yerr=[low, high], capsize=3, alpha=.95)
    ax.axhline(1, color="#596779", linestyle="--", linewidth=1)
    ax.set_xticks(range(6), [p + "\n" + s for p, s in settings])
    ax.set_ylabel("Speedup vs sequential")
    ax.set_ylim(.8, 2.35)
    ax.grid(axis="y", alpha=.2)
    ax.set_axisbelow(True)
    ax.set_title("Modeled read waits: 8 cores, c=2", pad=13)
    ax.legend(fontsize=10, loc="upper right")
    fig.text(.5, .055, "Cached: 256 MiB/op; c = requested connections (pool opens 2 at c=1). Sequential/overlap use c workers; batch pipeline uses 2c.",
             ha="center", fontsize=10, color="#415168")
    fig.text(.5, .032, "Latency: 16 MiB/op; independent read waits, no device queue or bandwidth cap. Error bars: 95% paired-run intervals.",
             ha="center", fontsize=10, color="#415168")
    fig.savefig(DEST / "plots/verify-simple.png", dpi=160)
    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plot", action="store_true", help="regenerate plots/verify-simple.png")
    args = parser.parse_args()
    pairs = [("cached", "overlap", "sequential"), ("cached", "optimized", "sequential"),
             ("pipeline", "optimized", "pipeline"), ("gp3", "overlap", "sequential"),
             ("i4i", "overlap", "sequential")]
    for profile, build, base in pairs:
        scopes = [("all", lambda r: True)]
        if profile == "cached":
            scopes += [("c=1", lambda r: r["concurrency"] == 1),
                       ("planned c", lambda r: r["concurrency"] != 1)]
        for scope, selected in scopes:
            print(f"{profile}: {build}/{base}, {scope}")
            for metric in ["MiB/s", "core-s/GiB", "allocs/op", "B/op"]:
                v, lo, hi = compare(profile, build, base, metric, selected)
                print(f"  {metric:12s} {v:+.1f}% ({lo:+.1f}% to {hi:+.1f}%)")
    if args.plot:
        plot()
