"""
dev_sweep.py  -  produces results/dev_results.csv from REAL Selective Repeat runs (localhost + dev_channel),
so the plotting pipeline can be built and tested before the full team framework exists.

!! This is DEV data (no real network delay, only SR). Final report data comes from Kartikey's
   automation using the same CSV columns.

CSV columns (agree these with Kartikey - plot_results.py depends on them):
  experiment, protocol, mode, seed, loss, reorder, window, rto, file_bytes,
  duration_s, goodput_kbps, retransmissions, timeouts, packets_sent, avg_rtt_ms
"""
import csv
import os
import random
import tempfile

from config import Config
from dev_channel import run_local_transfer
from rto import FixedRTO

FILE_BYTES = 150_000
SEEDS = [1, 2, 3]
DEFAULTS = dict(loss=0.05, reorder=0.0, window=8, rto=0.03)

SWEEPS = {                                   # experiment name -> (parameter, values)
    "loss_sweep":    ("loss",    [0.0, 0.02, 0.05, 0.10, 0.20]),
    "window_sweep":  ("window",  [1, 2, 4, 8, 16, 32]),
    "reorder_sweep": ("reorder", [0.0, 0.1, 0.2, 0.3, 0.4]),
    "rto_sweep":     ("rto",     [0.01, 0.02, 0.05, 0.1, 0.2]),
}
COLUMNS = ["experiment", "protocol", "mode", "seed", "loss", "reorder", "window", "rto", "file_bytes",
           "duration_s", "goodput_kbps", "retransmissions", "timeouts", "packets_sent", "avg_rtt_ms"]


def main(out_csv="results/dev_results.csv"):
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    with tempfile.TemporaryDirectory() as d, open(out_csv, "w", newline="") as fh:
        src, dst = os.path.join(d, "src"), os.path.join(d, "dst")
        with open(src, "wb") as f:
            f.write(random.Random(0).randbytes(FILE_BYTES))
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()

        for name, (param, values) in SWEEPS.items():
            for v in values:
                p = {**DEFAULTS, param: v}                       # change ONE thing, hold the rest constant
                for seed in SEEDS:
                    cfg = Config(initial_rto=p["rto"], min_rto=0.005, linger=0.3, max_retries=500)
                    tx, _ = run_local_transfer(src, dst, cfg, FixedRTO(p["rto"]), seed=seed,
                                               window=p["window"], loss=p["loss"], reorder=p["reorder"])
                    writer.writerow({"experiment": name, "protocol": "SR", "mode": "baseline", "seed": seed,
                                     "loss": p["loss"], "reorder": p["reorder"], "window": p["window"],
                                     "rto": p["rto"], "file_bytes": FILE_BYTES, **tx.to_row()})
                fh.flush()
                print(f"{name}: {param}={v} done")
    print("wrote", out_csv)


if __name__ == "__main__":
    main()
