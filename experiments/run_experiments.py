"""
Automated Experiment & Plotting Suite for DART Protocol Evaluation.
Course: CS-30003 CSE 34 | Catalogue Foundation: P3 Reliable Data Transfer over UDP

Generates the 4 required P3 experiments + DART Adaptive vs Static P3 claim:
  1. Goodput vs Loss Rate (Stop-and-Wait, Go-Back-N, Selective Repeat)
  2. Goodput vs Window Size (Go-Back-N, Selective Repeat)
  3. Retransmissions vs Reordering Probability (Go-Back-N, Selective Repeat)
  4. Sensitivity to Initial RTO Tuning
  5. DART Adaptive vs Static P3 Baseline under varying network degradation
"""

import os
import sys

# Ensure repository root is on Python module search path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import time
import json
import socket
import threading
import tempfile
import hashlib
from typing import List, Dict, Any, Tuple

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import numpy as np

from dart.arq import get_sender, get_receiver, ARQType
from dart.emulator.channel import EmulatorConfig
from dart.emulator.proxy import ChannelProxy
from dart.adaptive.engine import DARTAdaptiveEngine, FixedBaselineEngine
from experiments.benchmark import find_free_port

PLOTS_DIR = os.path.join(os.path.dirname(__file__), "plots")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(PLOTS_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)


def run_transfer_benchmark(
    arq_type: ARQType,
    payload_size_kb: int = 40,
    loss_rate: float = 0.0,
    delay_ms: float = 5.0,
    jitter_ms: float = 2.0,
    corrupt_rate: float = 0.0,
    reorder_rate: float = 0.0,
    window_size: int = 5,
    initial_rto: float = 0.3,
    adaptive: bool = False,
    seed: int = 42
) -> Dict[str, Any]:
    """Runs a single controlled transfer through the ChannelProxy and extracts metrics."""
    test_data = os.urandom(payload_size_kb * 1024)
    src_sha = hashlib.sha256(test_data).hexdigest()

    with tempfile.NamedTemporaryFile(delete=False) as f_src, tempfile.NamedTemporaryFile(delete=False) as f_dst:
        src_path = f_src.name
        dst_path = f_dst.name
        f_src.write(test_data)
        f_src.flush()

    rx_port = find_free_port()
    proxy_port = find_free_port()

    config = EmulatorConfig(
        loss_rate=loss_rate,
        delay_mean_ms=delay_ms,
        delay_jitter_ms=jitter_ms,
        corruption_rate=corrupt_rate,
        reorder_rate=reorder_rate,
        seed=seed
    )

    proxy = ChannelProxy(
        listen_host="127.0.0.1",
        listen_port=proxy_port,
        target_host="127.0.0.1",
        target_port=rx_port,
        forward_config=config
    )
    proxy.start()

    rx = get_receiver(arq_type, listen_host="127.0.0.1", listen_port=rx_port, window_size=window_size)
    engine = DARTAdaptiveEngine() if adaptive else FixedBaselineEngine(fixed_window=window_size)

    tx_kwargs = {
        "initial_rto": initial_rto,
        "min_rto": 0.02,
        "max_rto": 3.0
    }
    if arq_type != ARQType.STOP_AND_WAIT:
        tx_kwargs["window_size"] = window_size
        tx_kwargs["adaptive_engine"] = engine

    tx = get_sender(arq_type, host="127.0.0.1", port=proxy_port, **tx_kwargs)

    rx_thread = threading.Thread(target=lambda: rx.receive_file(dst_path, timeout_seconds=20.0), daemon=True)

    try:
        rx_thread.start()
        time.sleep(0.03)

        tx_stats = tx.send_file(src_path)
        rx_thread.join(timeout=10.0)

        with open(dst_path, "rb") as f:
            dst_data = f.read()

        dst_sha = hashlib.sha256(dst_data).hexdigest()
        integrity_ok = (src_sha == dst_sha)

        return {
            "goodput_kb_s": tx_stats.goodput_kb_s,
            "elapsed_s": tx_stats.elapsed_time,
            "packets_sent": tx_stats.total_packets_sent,
            "retransmissions": tx_stats.total_packets_retransmitted,
            "timeouts": tx_stats.total_timeouts,
            "avg_rtt_ms": tx_stats.avg_rtt_ms,
            "integrity_ok": integrity_ok
        }

    finally:
        proxy.stop()
        for p in (src_path, dst_path):
            if os.path.exists(p):
                os.remove(p)


def experiment_1_goodput_vs_loss():
    """Experiment 1: Goodput vs Loss Rate for SW, GBN, and SR."""
    print("Running Experiment 1: Goodput vs Loss Rate...")
    loss_rates = [0.0, 0.05, 0.10, 0.15, 0.20]
    protocols = [
        (ARQType.STOP_AND_WAIT, "Stop-and-Wait", "#ef4444", "o-"),
        (ARQType.GO_BACK_N, "Go-Back-N (W=8)", "#3b82f6", "s-"),
        (ARQType.SELECTIVE_REPEAT, "Selective Repeat (W=8)", "#10b981", "^-")
    ]

    results = {p[1]: [] for p in protocols}

    for rate in loss_rates:
        for proto_type, label, _, _ in protocols:
            res = run_transfer_benchmark(
                proto_type,
                payload_size_kb=30,
                loss_rate=rate,
                window_size=8,
                seed=42
            )
            results[label].append(res["goodput_kb_s"])

    plt.figure(figsize=(9, 5.5), dpi=150)
    for _, label, color, marker in protocols:
        plt.plot(np.array(loss_rates) * 100, results[label], marker, label=label, color=color, linewidth=2, markersize=7)

    plt.title("P3 Experiment 1: Goodput vs Packet Loss Rate", fontsize=14, fontweight="bold", pad=12)
    plt.xlabel("Packet Loss Rate (%)", fontsize=12)
    plt.ylabel("Goodput (KB/s)", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="#f8fafc", edgecolor="#cbd5e1")
    plt.tight_layout()
    out_path = os.path.join(PLOTS_DIR, "exp1_goodput_vs_loss.png")
    plt.savefig(out_path)
    plt.close()
    print(f"  -> Saved plot to {out_path}")


def experiment_2_goodput_vs_window():
    """Experiment 2: Goodput vs Window Size for GBN and SR."""
    print("Running Experiment 2: Goodput vs Window Size...")
    windows = [1, 2, 4, 8, 16]
    protocols = [
        (ARQType.GO_BACK_N, "Go-Back-N", "#3b82f6", "s-"),
        (ARQType.SELECTIVE_REPEAT, "Selective Repeat", "#10b981", "^-")
    ]

    results = {p[1]: [] for p in protocols}

    for w in windows:
        for proto_type, label, _, _ in protocols:
            res = run_transfer_benchmark(
                proto_type,
                payload_size_kb=40,
                loss_rate=0.08,
                delay_ms=8.0,
                window_size=w,
                seed=123
            )
            results[label].append(res["goodput_kb_s"])

    plt.figure(figsize=(9, 5.5), dpi=150)
    for _, label, color, marker in protocols:
        plt.plot(windows, results[label], marker, label=label, color=color, linewidth=2, markersize=7)

    plt.title("P3 Experiment 2: Goodput vs Window Size (8% Loss, 8ms Delay)", fontsize=14, fontweight="bold", pad=12)
    plt.xlabel("Window Size (Packets)", fontsize=12)
    plt.ylabel("Goodput (KB/s)", fontsize=12)
    plt.xticks(windows)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="#f8fafc", edgecolor="#cbd5e1")
    plt.tight_layout()
    out_path = os.path.join(PLOTS_DIR, "exp2_goodput_vs_window.png")
    plt.savefig(out_path)
    plt.close()
    print(f"  -> Saved plot to {out_path}")


def experiment_3_retransmissions_vs_reorder():
    """Experiment 3: Retransmissions vs Reordering Probability."""
    print("Running Experiment 3: Retransmissions vs Reorder Probability...")
    reorder_rates = [0.0, 0.05, 0.10, 0.15, 0.20]
    protocols = [
        (ARQType.GO_BACK_N, "Go-Back-N", "#3b82f6", "s-"),
        (ARQType.SELECTIVE_REPEAT, "Selective Repeat", "#10b981", "^-")
    ]

    results = {p[1]: [] for p in protocols}

    for ro in reorder_rates:
        for proto_type, label, _, _ in protocols:
            res = run_transfer_benchmark(
                proto_type,
                payload_size_kb=30,
                loss_rate=0.0,
                reorder_rate=ro,
                window_size=8,
                seed=555
            )
            results[label].append(res["retransmissions"])

    plt.figure(figsize=(9, 5.5), dpi=150)
    for _, label, color, marker in protocols:
        plt.plot(np.array(reorder_rates) * 100, results[label], marker, label=label, color=color, linewidth=2, markersize=7)

    plt.title("P3 Experiment 3: Retransmissions vs Packet Reordering Probability", fontsize=14, fontweight="bold", pad=12)
    plt.xlabel("Reordering Probability (%)", fontsize=12)
    plt.ylabel("Retransmission Count", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend(frameon=True, facecolor="#f8fafc", edgecolor="#cbd5e1")
    plt.tight_layout()
    out_path = os.path.join(PLOTS_DIR, "exp3_retransmissions_vs_reorder.png")
    plt.savefig(out_path)
    plt.close()
    print(f"  -> Saved plot to {out_path}")


def experiment_4_rto_sensitivity():
    """Experiment 4: Sensitivity to Initial RTO Tuning."""
    print("Running Experiment 4: Sensitivity to Initial RTO Tuning...")
    rto_values = [0.05, 0.1, 0.2, 0.5, 1.0, 1.5]
    goodputs = []

    for rto_val in rto_values:
        res = run_transfer_benchmark(
            ARQType.SELECTIVE_REPEAT,
            payload_size_kb=35,
            loss_rate=0.10,
            delay_ms=10.0,
            initial_rto=rto_val,
            window_size=6,
            seed=789
        )
        goodputs.append(res["goodput_kb_s"])

    plt.figure(figsize=(9, 5.5), dpi=150)
    plt.plot(rto_values, goodputs, "o-", color="#8b5cf6", linewidth=2.5, markersize=8)

    plt.title("P3 Experiment 4: Sensitivity to Initial RTO Tuning under 10% Loss", fontsize=14, fontweight="bold", pad=12)
    plt.xlabel("Initial RTO (seconds)", fontsize=12)
    plt.ylabel("Effective Goodput (KB/s)", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    out_path = os.path.join(PLOTS_DIR, "exp4_rto_sensitivity.png")
    plt.savefig(out_path)
    plt.close()
    print(f"  -> Saved plot to {out_path}")


def experiment_5_dart_vs_static_claim():
    """Experiment 5: Testing DART Claim - DART Adaptive vs Fixed P3 Baseline."""
    print("Running Experiment 5: DART Claim Validation (Adaptive vs Static)...")
    loss_scenarios = [0.02, 0.08, 0.15, 0.22]
    static_goodput = []
    dart_goodput = []
    static_retrans = []
    dart_retrans = []

    for l in loss_scenarios:
        # Static P3 (Fixed window = 4)
        s_res = run_transfer_benchmark(
            ARQType.SELECTIVE_REPEAT,
            payload_size_kb=45,
            loss_rate=l,
            delay_ms=10.0,
            window_size=4,
            adaptive=False,
            seed=999
        )
        static_goodput.append(s_res["goodput_kb_s"])
        static_retrans.append(s_res["retransmissions"])

        # DART Adaptive (Starts at 4, scales dynamically)
        d_res = run_transfer_benchmark(
            ARQType.SELECTIVE_REPEAT,
            payload_size_kb=45,
            loss_rate=l,
            delay_ms=10.0,
            window_size=4,
            adaptive=True,
            seed=999
        )
        dart_goodput.append(d_res["goodput_kb_s"])
        dart_retrans.append(d_res["retransmissions"])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5), dpi=150)
    loss_pct = np.array(loss_scenarios) * 100

    # Goodput comparison
    ax1.plot(loss_pct, static_goodput, "o--", label="Static P3 Baseline (W=4)", color="#64748b", linewidth=2)
    ax1.plot(loss_pct, dart_goodput, "s-", label="DART Adaptive Engine", color="#059669", linewidth=2.5)
    ax1.set_title("DART vs Static P3: Effective Goodput", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Packet Loss Rate (%)", fontsize=11)
    ax1.set_ylabel("Goodput (KB/s)", fontsize=11)
    ax1.grid(True, linestyle="--", alpha=0.5)
    ax1.legend()

    # Retransmissions comparison
    ax2.plot(loss_pct, static_retrans, "o--", label="Static P3 Baseline (W=4)", color="#64748b", linewidth=2)
    ax2.plot(loss_pct, dart_retrans, "s-", label="DART Adaptive Engine", color="#059669", linewidth=2.5)
    ax2.set_title("DART vs Static P3: Retransmission Overhead", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Packet Loss Rate (%)", fontsize=11)
    ax2.set_ylabel("Retransmissions (Count)", fontsize=11)
    ax2.grid(True, linestyle="--", alpha=0.5)
    ax2.legend()

    plt.suptitle("DART Extension Claim Verification: Adaptive Transmission vs Fixed Baseline", fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    out_path = os.path.join(PLOTS_DIR, "exp5_dart_vs_static_claim.png")
    plt.savefig(out_path)
    plt.close()
    print(f"  -> Saved plot to {out_path}")


def run_all_experiments():
    print("=" * 60)
    print("DART CS-30003 Automated Experimentation Suite")
    print("=" * 60)
    start_time = time.time()
    experiment_1_goodput_vs_loss()
    experiment_2_goodput_vs_window()
    experiment_3_retransmissions_vs_reorder()
    experiment_4_rto_sensitivity()
    experiment_5_dart_vs_static_claim()
    print(f"All 5 experiment suites completed successfully in {time.time() - start_time:.2f} seconds!")


if __name__ == "__main__":
    run_all_experiments()
