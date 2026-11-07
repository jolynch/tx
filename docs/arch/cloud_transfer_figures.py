"""Transfer comparisons for CLOUD.md; rendered by cloud_figures.py --plot."""

import numpy as np

import cloud_figures as m


STRATEGIES = {"none": "No compression", "lz4": "Fixed lz4",
              "zstd": "Fixed zstd -1", "tx": "Adaptive tx"}
# Title, data, output name, and expected codec after the network clamp.
DATASETS = (
    ("Incompressible files", m.INCOMPRESSIBLE, "cloud-burst-incompressible", "none"),
    ("lz4-compressed database segments", m.LZ4_COMPRESSED, "cloud-burst", "zstd"),
    ("Compressible files", m.COMPRESSIBLE, "cloud-burst-compressible", "lz4"),
)


def validate_incompressible(case):
    """Fixed codecs spend CPU without saving wire bytes; tx stays uncompressed."""
    assert set(case["tx"]["mode"]) == {"none"}
    assert np.isclose(case["tx"]["done"], case["none"]["done"])
    assert case["none"]["rate"][0] > case["lz4"]["rate"][0] > case["zstd"]["rate"][0]
    assert case["none"]["done"] < case["lz4"]["done"] < case["zstd"]["done"]


def burst_case(burst_s, data=m.LZ4_COMPRESSED):
    """Secure transfer of 80% of the sender SSD to an empty receiver.

    Cold sender reads bound logical bytes for every codec, independently of
    wire savings. Codecs share every vCPU with the rest of the pipeline,
    including AES-256-GCM. The pipeline rate is an assumption, not an AES
    primitive benchmark or a measurement of tx on this hardware.
    """
    sender, receiver = m.I4I["4xlarge"], m.I7I_4XLARGE

    def wire(t):
        key = "net_burst" if t < burst_s else "net_base"
        return m.gbps_to_mibps(min(sender[key], receiver[key]))

    return {s: m.simulate(s, wire, m.RECEIVER_WRITE_MIBPS, m.dirty_limits("4xlarge"),
                         sender["vcpu"], m.F1_CONNS, m.F1_SECONDS, data,
                         m.F1_DATASET_GIB * m.GIB, sender_read_mibps=m.SENDER_READ_MIBPS,
                         decode_vcpus=receiver["vcpu"], plain_share=m.F1_PLAN["plain_share"],
                         window_mib=m.F1_PLAN["batch_mib"])
            for s in (*STRATEGIES, "optimal")}


def validate_case(case, seconds, data):
    assert all(r["done"] is not None for r in case.values())
    baseline = m.gbps_to_mibps(m.I7I_4XLARGE["net_base"])
    assert m.RECEIVER_WRITE_MIBPS > m.SENDER_READ_MIBPS
    for r in case.values():
        assert np.max(r["rate"]) <= m.SENDER_READ_MIBPS
        assert np.isclose(np.dot(r["rate"], r["t1"] - r["t0"]),
                          m.F1_DATASET_GIB * m.GIB, rtol=1e-9)
    # tx never beats the oracle and never loses to sending none; it gains
    # on none only if its files switch before they end.
    assert case["tx"]["done"] >= case["optimal"]["done"] * (1 - 1e-9)
    assert case["tx"]["done"] <= case["none"]["done"] * 1.001
    if data is not m.INCOMPRESSIBLE:
        if m.F1_PLAN["plain_share"] < 1:
            assert case["tx"]["done"] < case["none"]["done"]
            after = case["tx"]["rate"][case["tx"]["t0"] > seconds + 30]
            assert np.max(after) > baseline
    else:
        validate_incompressible(case)
        assert abs(case["tx"]["done"] / case["none"]["done"] - 1) < .001
        for r in case.values():
            assert np.max(r["rate"][r["t0"] >= seconds]) <= baseline


def xlarge_case(data):
    """Matched i4i.xlarge pair, cold sender reads, and a 10-minute burst."""
    sender = receiver = m.I4I["xlarge"]
    plan = m.client_plan(sender["vcpu"], min(sender["net_burst"], receiver["net_burst"]))
    total = m.I4I_XLARGE_NVME_GIB * m.COPY_FRACTION * m.GIB
    baseline = m.gbps_to_mibps(receiver["net_base"])

    def wire(t):
        key = "net_burst" if t < m.BURST_MIN * 60 else "net_base"
        return m.gbps_to_mibps(min(sender[key], receiver[key]))

    case = {s: m.simulate(s, wire, m.NVME_WRITE_XLARGE, m.dirty_limits("xlarge"),
                         sender["vcpu"], plan["conns"], m.F1_SECONDS, data, total,
                         sender_read_mibps=m.NVME_READ_XLARGE,
                         decode_vcpus=receiver["vcpu"], plain_share=plan["plain_share"],
                         window_mib=plan["batch_mib"])
            for s in (*STRATEGIES, "optimal")}
    for result in case.values():
        assert result["done"] is not None
        assert np.isclose(np.dot(result["rate"], result["t1"] - result["t0"]), total, rtol=1e-9)
        assert np.max(result["rate"]) <= m.NVME_READ_XLARGE
    assert case["tx"]["done"] >= case["optimal"]["done"] * (1 - 1e-9)
    assert case["tx"]["done"] <= case["none"]["done"] * 1.001
    if data is not m.INCOMPRESSIBLE:
        if plan["plain_share"] < 1:
            late = case["tx"]["rate"][case["tx"]["t0"] > m.BURST_MIN * 60 + 60]
            assert np.max(late) > baseline
    else:
        validate_incompressible(case)
        for result in case.values():
            assert np.max(result["rate"][result["t0"] >= m.BURST_MIN * 60]) <= baseline
    return case


def completion_table():
    """Minutes and percentage gaps to the best modeled secure codec switch.

    Rsync rows are optimistic network-only floors, not measured performance.
    One ordinary SSH/TCP flow is capped at 5 Gbps outside a cluster placement
    group, without ENA Express. Compression can increase logical throughput.
    """
    cases = [burst_case(seconds) for seconds in (m.BURST_MIN * 60, m.SHORT_BURST_S)]
    best = [c["optimal"]["done"] / 60 for c in cases]
    total = m.F1_DATASET_GIB * m.GIB
    ssh = m.gbps_to_mibps(5)
    ratio = m.LZ4_COMPRESSED["ratio"]["zstd"]
    rows = [("Best modeled codec switch, AES-GCM", best, False),
            ("**tx adaptive, AES-GCM**", [c["tx"]["done"] / 60 for c in cases], False),
            ("tx no compression, AES-GCM", [c["none"]["done"] / 60 for c in cases], False),
            ("tx fixed lz4, AES-GCM", [c["lz4"]["done"] / 60 for c in cases], False),
            ("tx fixed zstd -1, AES-GCM", [c["zstd"]["done"] / 60 for c in cases], False),
            ("rsync, plaintext, no compression", [total / ssh / 60] * 2, True),
            ("rsync/SSH, AES-GCM, no compression", [total / ssh / 60] * 2, True),
            (f"rsync/SSH, AES-GCM, ideal {ratio:.3g}× compression",
             [total / (ssh * ratio) / 60] * 2, True)]
    out = ["| Method | 10 min network burst | 30 s network burst |",
           "| --- | ---: | ---: |"]
    for name, times, floor in rows:
        cells = []
        for t, b in zip(times, best):
            pct = 100 * (t / b - 1)
            if not floor:
                cells.append(f"{t:.1f} min (+{pct:.1f}%)")
            elif pct < 0:
                cells.append(f"≥{t:.1f} min (up to {-pct:.1f}% faster)")
            else:
                cells.append(f"≥{t:.1f} min (≥{pct:.1f}% slower)")
        if name.startswith("**"):
            cells = [f"**{c}**" for c in cells]
        out.append("| " + " | ".join([name, *cells]) + " |")
    return "\n".join(out)


def receiver_case(size):
    """Uncompressed copy between matching instance sizes, into an empty SSD."""
    small = size == "xlarge"
    receiver = m.I4I["xlarge"] if small else m.I7I_4XLARGE
    sender = m.I4I[size]
    sender_read = m.NVME_READ_XLARGE if small else m.SENDER_READ_MIBPS
    capacity = m.I4I_XLARGE_NVME_GIB if small else m.LOCAL_4XLARGE_GIB
    disk = m.NVME_WRITE_XLARGE if small else m.RECEIVER_WRITE_MIBPS
    baseline = m.gbps_to_mibps(min(sender["net_base"], receiver["net_base"]))
    burst = m.gbps_to_mibps(min(sender["net_burst"], receiver["net_burst"]))
    total = capacity * m.COPY_FRACTION * m.GIB

    def wire(t):
        return burst if t < m.BURST_MIN * 60 else baseline

    result = m.simulate("none", wire, disk, m.dirty_limits(size), sender["vcpu"],
                        m.F1_CONNS, m.F1_SECONDS, total_mib=total,
                        sender_read_mibps=sender_read, decode_vcpus=receiver["vcpu"])
    assert result["done"] > 12 * 60
    assert m.cum_at(result, 12 * 60) < total
    assert np.all(result["rate"][result["t0"] >= m.BURST_MIN * 60] == baseline)
    if small:
        fill = m.first_drop(result)
        assert 90 < fill < 240
        assert np.isclose(np.interp(300, result["t0"], result["rate"]), disk)
        assert burst > sender_read > disk > baseline
        assert result["rate"][0] == sender_read
    else:
        assert np.all(result["rate"][result["t0"] < m.BURST_MIN * 60] == m.SENDER_READ_MIBPS)
        assert disk > m.SENDER_READ_MIBPS
    return result, disk, baseline, burst


def completion_savings(case):
    """Minutes and percent saved against each fixed strategy.

    Each percentage uses that fixed strategy's receive time as the denominator.
    Completion is the last received byte, excluding final sync and verification.
    """
    return {mode: ((case[mode]["done"] - case["tx"]["done"]) / 60,
                   100 * (1 - case["tx"]["done"] / case[mode]["done"]))
            for mode in m.LADDER}


def completion_window(results):
    finishes = [r["done"] / 60 for r in results]
    return 5 * np.floor(min(finishes) / 5) - 1, max(finishes) + 1.5


def panel_axes(axes, size, finishes):
    """Separate the initial burst, network clamp, and receive completion."""
    small = size == "xlarge"
    early, clamp, finish = axes
    baseline = m.gbps_to_mibps(m.I4I[size]["net_base"])
    burst = m.gbps_to_mibps(m.I4I[size]["net_burst"])
    ticks = ([0, baseline, m.NVME_WRITE_XLARGE, m.NVME_READ_XLARGE, burst]
             if small else [0, baseline, m.SENDER_READ_MIBPS])
    for col, ax in enumerate(axes):
        ax.set_ylim(0, 1450 if small else 3500)
        ax.set_yticks(ticks, [f"{v:,.0f}" for v in ticks])
        ax.tick_params(labelsize=10)
        ax.set_xlabel("Elapsed time (s)" if col == 0 else "Elapsed time (min)", fontsize=11)
        if col:
            ax.spines["left"].set_visible(False)
            ax.tick_params(left=False, labelleft=False)
    early_end = 240 if small else 90
    early.set(xlim=(0, early_end), xticks=[0, 60, 120, 180, 240] if small else [0, 30, 60, 90])
    clamp.set(xlim=(8, 12), xticks=[8, 10, 12])
    lo, hi = completion_window(finishes)
    finish.set_xlim(lo, hi)
    from matplotlib.ticker import MaxNLocator
    finish.xaxis.set_major_locator(MaxNLocator(nbins=4, integer=True))
    m.break_marks(early, clamp)
    m.break_marks(clamp, finish)
    return early_end, lo, hi


def network_limits(ax, baseline, burst, scale):
    """Connect burst bandwidth to a baseline reference visible throughout."""
    clamp = m.BURST_MIN * 60 / scale
    ax.axhline(baseline, **m.REFLINE["network_baseline"])
    ax.plot([0, clamp, clamp], [burst, burst, baseline], **m.REFLINE["network_burst"])


def phase(ax, lo, hi, title, color, top):
    ax.axvspan(lo, hi, color=color, alpha=.09, zorder=0)
    m.label(ax, (lo + hi) / 2, top, title, ha="center", va="top", fontsize=10, color=m.MUTED)


def finish_marker(ax, result, strategy):
    time = result["done"] / 60
    rate = result["rate"][-1]
    ax.plot(time, rate, marker="o", ms=6 if strategy == "tx" else 4,
            color=m.STRATEGY[strategy]["color"], zorder=8 if strategy == "tx" else 7)


def figure_phases():
    """Compare matched pairs through uncompressed receive completion."""
    from matplotlib.lines import Line2D

    fig = m.new_figure(6.4, width=14)
    fig.supxlabel("Each copy fills 80% of its receiver's SSD. Nominal disk rates; fullness effects unmeasured.\n"
                  "Receive completion excludes final sync and verification. Model, not benchmark.",
                  x=.995, ha="right", fontsize=9.5, color=m.MUTED, fontstyle="italic")
    axes = fig.subplots(2, 3, sharey="row", gridspec_kw={"width_ratios": [.85, 1.2, 1.15]})
    handles = [Line2D([], [], color=m.INK, lw=2, label="Uncompressed throughput"),
               Line2D([], [], **m.REFLINE["network_burst"], label="Burst network bandwidth"),
               Line2D([], [], **m.REFLINE["network_baseline"], label="Baseline network bandwidth")]
    fig.legend(handles=handles, loc="outside upper center", ncol=3, fontsize=10,
               title="Matched instance sizes · cold sender reads · AES-256-GCM",
               title_fontsize=12)

    for row, size in enumerate(("xlarge", "4xlarge")):
        result, disk, baseline, burst = receiver_case(size)
        small = size == "xlarge"
        early, clamp, finish = axes[row]
        top = 1390 if small else 3330
        early_end, lo, hi = panel_axes(axes[row], size, [result])
        xs, ys = m.rate_series(result, bin_s=m.F1_BIN_S)
        for col, ax in enumerate(axes[row]):
            scale = 1 if col == 0 else 60
            ax.plot(xs / scale, ys, color=m.INK, lw=2,
                    drawstyle="steps-post", zorder=3)
            network_limits(ax, baseline, burst, scale)
        early.set_ylabel("Throughput (MiB/s)", fontsize=11)
        pair = "i4i.xlarge → i4i.xlarge" if small else "i4i.4xlarge → i7i.4xlarge"
        early.set_title(f"({chr(97 + row * 3)}) {pair}", fontsize=11)
        clamp.set_title(f"({chr(98 + row * 3)}) Network clamp", fontsize=11)
        finish.set_title(f"({chr(99 + row * 3)}) Receive completion", fontsize=11)
        if small:
            fill = m.first_drop(result)
            phase(early, 0, fill, "Memory buffering", m.BLUE, top)
            phase(early, fill, early_end, "Disk writes", m.GREEN, top)
            early.axvline(fill, ymax=.8, color=m.REF, lw=.8, ls=":")
            m.label(early, 8, 850, "Sender reads: 691 MiB/s\nReceiver writes: 525 MiB/s",
                    fontsize=10, va="bottom")
            m.label(early, fill + 3, 1000, f"Write throttling\n≈{fill:.0f} s", fontsize=10, va="top")
        else:
            phase(early, 0, 90, "Sender disk reads", m.GREEN, top)
            m.label(early, 87, 2000, "Receiver disk writes exceed\nSender reads: no write throttling",
                    ha="right", va="top", fontsize=10)
        phase(clamp, 8, m.BURST_MIN, "Disk writes" if small else "Disk reads", m.GREEN, top)
        phase(clamp, m.BURST_MIN, 12, "Baseline\nnetwork", m.ORANGE, top)
        phase(finish, lo, hi, "Baseline network", m.ORANGE, top)
        clamp.axvline(m.BURST_MIN, ymax=.8, color=m.REF, lw=.8, ls=":")
        done = result["done"] / 60
        finish.plot(done, result["rate"][-1], "o", color=m.INK, ms=6, zorder=8)
        finish.axvline(done, ymax=.65, color=m.REF, lw=.8, ls=":")
        m.label(finish, (lo + hi) / 2, top * .8, f"Uncompressed complete: {done:.1f} min",
                ha="center", va="top", fontsize=10.5)
    return m.save(fig, "cloud-phases")


def codec_strip(axes, result):
    """Show the modeled settled codec; per-file warm-up is blended into rates."""
    modes = np.array([m.LADDER.index(mode) for mode in result["home"]])
    times = np.append(result["t0"], result["t1"][-1])
    values = np.append(modes, modes[-1])
    for col, ax in enumerate(axes):
        scale = 1 if col == 0 else 60
        for level in range(len(m.LADDER)):
            ax.axhline(level, color=m.REF, lw=.7, ls=(0, (4, 3)), alpha=.6, zorder=0)
        ax.step(times / scale, values, where="post", color=m.BLUE, lw=1.5)
        ax.set_ylim(-.35, 2.35)
        ax.set_yticks(range(len(m.LADDER)), [m.CODEC_LABEL[mode] for mode in m.LADDER])
        ax.tick_params(axis="y", labelsize=9, length=0, labelleft=True)
        ax.tick_params(axis="x", labelsize=10)
        ax.set_xlabel("Elapsed time (s)" if col == 0 else "Elapsed time (min)", fontsize=11)
        ax.spines["left"].set_visible(False)
        ax.grid(False)
    axes[0].set_ylabel("tx codec", fontsize=10)
    m.break_marks(axes[0], axes[1])
    m.break_marks(axes[1], axes[2])


def figure_burst():
    from matplotlib.lines import Line2D

    paths = list(figure_phases())
    for dataset, data, name, expected in DATASETS:
        fig = m.new_figure(8.0, width=14)
        fig.supxlabel("Shading follows uncompressed transfers; row scales differ. Codec strips show settled choices; rates include recovery checks. Differences: fixed − tx, as % of fixed time.\n"
                      "Copies fill 80% of each SSD; fullness effects unmeasured. Codec CPU rates held fixed across datasets. Completion excludes sync and verification. Model, not benchmark.",
                      x=.995, ha="right", fontsize=9.5, color=m.MUTED, fontstyle="italic")
        grid = fig.add_gridspec(4, 3, width_ratios=[.85, 1.2, 1.15],
                                height_ratios=[4.5, 1.1, 4.5, 1.1], hspace=.08)
        axes, strips = [], []
        for row in range(2):
            panels, codecs = [], []
            for col in range(3):
                ax = fig.add_subplot(grid[row * 2, col], sharey=panels[0] if col else None)
                panels.append(ax)
                codecs.append(fig.add_subplot(grid[row * 2 + 1, col], sharex=ax))
            axes.append(panels)
            strips.append(codecs)
        handles = [Line2D([], [], **m.STRATEGY[s], label=name) for s, name in STRATEGIES.items()]
        handles.extend([
            Line2D([], [], **m.REFLINE["network_burst"], label="Burst network bandwidth"),
            Line2D([], [], **m.REFLINE["network_baseline"], label="Baseline network bandwidth"),
        ])
        ratios = "; ".join(f"{m.CODEC_LABEL[mode]} {data['ratio'][mode]:.3g}:1" for mode in ("lz4", "zstd"))
        fig.legend(handles=handles, loc="outside upper center", ncol=6, fontsize=10,
                   title=f"{dataset} ({ratios}) · matched instance sizes\n"
                         f"Cold sender reads · {m.FILE_MIB:g} MiB files · AES-256-GCM · 10-minute network burst",
                   title_fontsize=11)

        for row, size in enumerate(("xlarge", "4xlarge")):
            small = size == "xlarge"
            case = xlarge_case(data) if small else burst_case(m.BURST_MIN * 60, data)
            if not small:
                validate_case(case, m.BURST_MIN * 60, data)
            assert all(r["done"] > 12 * 60 for r in case.values())
            plan = m.client_plan(m.I4I[size]["vcpu"], m.I4I[size]["net_burst"])
            settled = expected if plan["plain_share"] < 1 else "none"
            tail = np.array(case["tx"]["home"])[case["tx"]["t0"] >= case["tx"]["done"] - 60]
            assert set(tail) == {settled}, (dataset, size, set(tail), settled)
            receiver = m.I4I["xlarge"] if small else m.I7I_4XLARGE
            baseline, burst = (m.gbps_to_mibps(receiver[k]) for k in ("net_base", "net_burst"))
            top = 1390 if small else 3330
            early, clamp, finish = axes[row]
            early_end, lo, hi = panel_axes(axes[row], size, list(case.values()))
            for ax in axes[row]:
                ax.set_xlabel("")
                ax.tick_params(axis="x", labelbottom=False)
            codec_strip(strips[row], case["tx"])
            series = {strategy: m.rate_series(case[strategy], bin_s=m.F1_BIN_S)
                      for strategy in STRATEGIES}
            for col, ax in enumerate(axes[row]):
                scale = 1 if col == 0 else 60
                for strategy in ("tx", "none", "lz4", "zstd"):
                    xs, ys = series[strategy]
                    m.line(ax, xs / scale, ys, strategy,
                           drawstyle="steps-post")
                network_limits(ax, baseline, burst, scale)
            early.set_ylabel("Logical throughput (MiB/s)", fontsize=11)
            gib = m.I4I_XLARGE_NVME_GIB if small else m.LOCAL_4XLARGE_GIB
            pair = "i4i.xlarge → i4i.xlarge" if small else "i4i.4xlarge → i7i.4xlarge"
            early.set_title(f"({chr(97 + row * 3)}) {pair} · {gib * m.COPY_FRACTION:,.0f} GiB", fontsize=11)
            clamp.set_title(f"({chr(98 + row * 3)}) Network clamp", fontsize=11)
            finish.set_title(f"({chr(99 + row * 3)}) Receive completion", fontsize=11)
            if small:
                fill = m.first_drop(case["none"])
                phase(early, 0, fill, "Memory buffering", m.BLUE, top)
                phase(early, fill, early_end, "Disk writes", m.GREEN, top)
                early.axvline(fill, ymax=.8, color=m.REF, lw=.8, ls=":")
                m.label(early, fill + 3, 1050, f"Write throttling\n≈{fill:.0f} s", fontsize=10, va="top")
            else:
                phase(early, 0, 90, "Sender disk reads", m.GREEN, top)
            phase(clamp, 8, m.BURST_MIN, "Disk writes" if small else "Disk reads", m.GREEN, top)
            phase(clamp, m.BURST_MIN, 12, "Baseline\nnetwork", m.ORANGE, top)
            phase(finish, lo, hi, "Baseline network", m.ORANGE, top)
            clamp.axvline(m.BURST_MIN, ymax=.8, color=m.REF, lw=.8, ls=":")
            savings = completion_savings(case)
            for strategy in ("none", "lz4", "zstd", "tx"):
                finish_marker(finish, case[strategy], strategy)
            tx_done = case["tx"]["done"] / 60
            finish.axvline(tx_done, ymax=.5, color=m.BLUE, lw=.8, ls=":")
            label_x = lo + .34 * (hi - lo)
            label_style = dict(va="top", fontsize=10, fontfamily="DejaVu Sans Mono")
            m.label(finish, label_x, top * .89, "tx (adapt):",
                    ha="right", color=m.INK, **label_style)
            m.label(finish, label_x, top * .89, f" {tx_done:5.1f} min",
                    ha="left", color=m.BLUE, **label_style)
            for i, (mode, (saved, percent)) in enumerate(savings.items()):
                # The model can tie exactly; avoid a signed zero in the figure.
                saved = 0.0 if round(saved, 1) == 0 else saved
                percent = 0.0 if round(percent, 1) == 0 else percent
                minutes = f"{saved:+5.1f}" if saved else f"{0.0:5.1f}"
                pct = f"{percent:+5.1f}" if percent else f"{0.0:5.1f}"
                y = top * (.80 - .065 * i)
                m.label(finish, label_x, y, f"{m.CODEC_LABEL[mode]}:",
                        ha="right", color=m.INK, **label_style)
                color = "firebrick" if saved or percent else m.MUTED
                m.label(finish, label_x, y, f" {minutes} min ({pct}%)",
                        ha="left", color=color, **label_style)
            late = case["tx"]["t0"] > 11 * 60
            durations = case["tx"]["t1"][late] - case["tx"]["t0"][late]
            rate = np.average(case["tx"]["rate"][late], weights=durations)
            if data is not m.INCOMPRESSIBLE:
                m.label(clamp, 11, top * .78,
                        f"Adaptive: {rate:,.0f} MiB/s logical\nNetwork: {baseline:,.0f} MiB/s",
                        ha="center", va="top", fontsize=9)
                final = case["tx"]["home"][-1]
                if final == "none":
                    note = (f"{m.FILE_MIB:,.0f} MiB files end before\n"
                            f"their windows switch: {rate:,.0f} MiB/s")
                else:
                    fixed = np.median(case[final]["rate"][case[final]["t0"] > 11 * 60])
                    note = (f"{m.CODEC_LABEL[final]} after each file's first {100 * plan['plain_share']:g}%\n"
                            f"{rate:,.0f} vs fixed {m.CODEC_LABEL[final]} {fixed:,.0f} MiB/s")
                level = m.LADDER.index(final)
                m.arrow(strips[row][2], note,
                        xy=(tx_done, level), xytext=(.68, .45), textcoords="axes fraction",
                        ha="center", va="center", fontsize=8.5,
                        bbox=dict(facecolor="white", edgecolor="none", pad=1.5))
            else:
                m.label(clamp, 11, top * .78,
                        "Compression saves 0 wire bytes\nSame baseline rate; extra CPU",
                        ha="center", va="top", fontsize=9)
            print(f"{dataset}, {pair}: tx={tx_done:.3f} min; "
                  + ", ".join(f"saved vs {mode}={saved:.3f} min ({pct:.3f}%)"
                              for mode, (saved, pct) in savings.items()))
        paths.extend(m.save(fig, name))
    print(completion_table())
    return paths
