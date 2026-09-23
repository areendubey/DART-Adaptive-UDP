"""
plot_results.py  -  turns the experiment CSV into report-ready figures.   (OWNER: Savvy)

  python plot_results.py results/dev_results.csv            -> results/figures/*.png

How it works: PLOTS is a table. One row = one figure. To add a graph, add a row - no new code.
Each point = mean over seeds, error bar = standard deviation across seeds.
Experiments with no rows in the CSV are skipped, so this runs on partial data too.

DART vs baseline: rows with mode="baseline" (fixed P3) and mode="dart" share an experiment name
("dart_vs_baseline"); lines are split by the `mode` column.
"""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")                         # draw to files, no window needed
import matplotlib.pyplot as plt
import pandas as pd

# (experiment, x column, y column, title, x label, y label, one line per ..., output name)
PLOTS = [
    ("loss_sweep",       "loss",    "goodput_kbps",    "Goodput vs loss rate",           "Loss probability",      "Goodput (kbit/s)", "protocol", "goodput_vs_loss"),
    ("window_sweep",     "window",  "goodput_kbps",    "Goodput vs window size",         "Window size (packets)", "Goodput (kbit/s)", "protocol", "goodput_vs_window"),
    ("reorder_sweep",    "reorder", "retransmissions", "Retransmissions vs reordering",  "Reorder probability",   "Retransmissions",  "protocol", "retx_vs_reorder"),
    ("rto_sweep",        "rto",     "goodput_kbps",    "Sensitivity to RTO tuning",      "Fixed RTO (s)",         "Goodput (kbit/s)", "protocol", "goodput_vs_rto"),
    ("dart_vs_baseline", "loss",    "goodput_kbps",    "DART vs fixed baseline: goodput", "Loss probability",     "Goodput (kbit/s)", "mode",     "dart_goodput"),
    ("dart_vs_baseline", "loss",    "retransmissions", "DART vs fixed baseline: retransmissions", "Loss probability", "Retransmissions", "mode",  "dart_retx"),
]


def plot_one(df, spec, outdir: Path):
    experiment, x, y, title, xlabel, ylabel, group, name = spec
    d = df[df["experiment"] == experiment]
    if d.empty:
        print(f"skip {name}: no '{experiment}' rows")
        return
    fig, ax = plt.subplots(figsize=(6, 4))
    for label, g in d.groupby(group):
        s = g.groupby(x)[y].agg(["mean", "std"]).fillna(0)          # average over seeds
        ax.errorbar(s.index, s["mean"], yerr=s["std"], marker="o", capsize=3, label=label)
    ax.set(title=title, xlabel=xlabel, ylabel=ylabel)
    pos = d[y][d[y] > 0]
    if len(pos) and pos.max() / pos.min() > 50:                   # huge spread -> log axis stays readable
        ax.set_yscale("log")
    ax.grid(alpha=0.3)
    ax.legend(title=group)
    fig.tight_layout()
    fig.savefig(outdir / f"{name}.png", dpi=150)
    plt.close(fig)
    print("saved", outdir / f"{name}.png")


def plot_window_trace(trace_csv: str, outdir: Path):
    """Window-vs-time for DART. CSV columns: run_id, t, window  (from Stats.window_trace)."""
    df = pd.read_csv(trace_csv)
    fig, ax = plt.subplots(figsize=(6, 3.5))
    for run_id, g in df.groupby("run_id"):
        ax.step(g["t"], g["window"], where="post", label=str(run_id))
    ax.set(title="Sender window over time", xlabel="Time (s)", ylabel="Window (packets)")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(outdir / "window_trace.png", dpi=150)
    plt.close(fig)


def main(csv_path: str):
    df = pd.read_csv(csv_path)
    outdir = Path(csv_path).parent / "figures"
    outdir.mkdir(parents=True, exist_ok=True)
    for spec in PLOTS:
        plot_one(df, spec, outdir)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results/dev_results.csv")
