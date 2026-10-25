"""
DART: Dynamic Adaptive Reliable Transport
Unified Command-Line Interface (CLI)

Supports:
  - File transmission with Stop-and-Wait, Go-Back-N, or Selective Repeat
  - Receiving with automatic SHA-256 integrity computation
  - Standalone Network Channel Emulator proxy with configurable loss, delay, corruption, reordering
  - Automated benchmark suite execution
  - Interactive Web Dashboard server launch
"""

import sys
import os
import argparse
import hashlib
import time

# Ensure repository root is in search path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dart.arq import get_sender, get_receiver, ARQType
from dart.emulator.channel import EmulatorConfig
from dart.emulator.proxy import ChannelProxy
from dart.adaptive.engine import DARTAdaptiveEngine, FixedBaselineEngine
from dart.stats import TransferStats


def calculate_sha256(filepath: str) -> str:
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def handle_send(args):
    filepath = args.file
    if not os.path.exists(filepath):
        print(f"Error: File '{filepath}' does not exist.")
        sys.exit(1)

    file_size = os.path.getsize(filepath)
    file_sha = calculate_sha256(filepath)
    print("=" * 65)
    print("  DART Reliable UDP File Sender")
    print("=" * 65)
    print(f"Target:       {args.host}:{args.port}")
    print(f"Protocol:     {args.arq.upper()}")
    print(f"File:         {filepath} ({file_size:,} bytes)")
    print(f"SHA-256:      {file_sha}")
    print(f"Window Size:  {args.window}")
    print(f"Initial RTO:  {args.rto:.3f}s")
    print(f"Adaptive:     {'YES (DART Engine)' if args.adaptive else 'NO (Fixed Baseline)'}")
    print("-" * 65)

    adaptive_engine = DARTAdaptiveEngine() if args.adaptive else FixedBaselineEngine(fixed_window=args.window)

    sender = get_sender(
        args.arq,
        host=args.host,
        port=args.port,
        window_size=args.window,
        initial_rto=args.rto,
        adaptive_engine=adaptive_engine
    )

    start_time = time.time()
    try:
        stats: TransferStats = sender.send_file(filepath)
        elapsed = time.time() - start_time
        print("\n" + "=" * 65)
        print("  Transfer Complete - Sender Statistics")
        print("=" * 65)
        print(f"Duration:           {stats.elapsed_seconds:.3f} s")
        print(f"Data Packets Sent:  {stats.data_packets_sent} (Total: {stats.packets_sent})")
        print(f"Retransmissions:    {stats.packets_retransmitted}")
        print(f"ACKs Received:      {stats.acks_received}")
        print(f"Timeouts:           {stats.timeouts}")
        print(f"Throughput:         {stats.throughput_kbps:.2f} KB/s")
        print(f"Goodput:            {stats.goodput_kbps:.2f} KB/s")
        print(f"Mean RTT:           {stats.mean_rtt_ms:.2f} ms")
        print(f"Source SHA-256:     {stats.sha256}")
        print("=" * 65)
    except KeyboardInterrupt:
        print("\nTransfer aborted by user.")
    except Exception as e:
        print(f"\nError during transfer: {e}")
        sys.exit(1)


def handle_recv(args):
    out_path = args.output
    print("=" * 65)
    print("  DART Reliable UDP File Receiver")
    print("=" * 65)
    print(f"Listening:    {args.host}:{args.port}")
    print(f"Protocol:     {args.arq.upper()}")
    print(f"Output File:  {out_path}")
    print(f"Timeout:      {args.timeout}s")
    print("-" * 65)
    print("Waiting for sender connection...")

    receiver = get_receiver(
        args.arq,
        listen_host=args.host,
        listen_port=args.port,
        window_size=args.window
    )

    try:
        stats: TransferStats = receiver.receive_file(out_path, timeout_seconds=args.timeout)
        recv_sha = calculate_sha256(out_path)
        print("\n" + "=" * 65)
        print("  Transfer Received Successfully")
        print("=" * 65)
        print(f"Output Size:        {os.path.getsize(out_path):,} bytes")
        print(f"Duration:           {stats.elapsed_seconds:.3f} s")
        print(f"Packets Received:   {stats.packets_received}")
        print(f"Duplicates:         {stats.packets_duplicated}")
        print(f"Corrupt Discards:   {stats.packets_corrupted}")
        print(f"ACKs Sent:          {stats.acks_sent}")
        print(f"Goodput:            {stats.goodput_kbps:.2f} KB/s")
        print(f"Received SHA-256:   {recv_sha}")
        print("=" * 65)
    except TimeoutError:
        print("\nReceiver timed out waiting for data.")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nReceiver stopped by user.")
    except Exception as e:
        print(f"\nError during reception: {e}")
        sys.exit(1)


def handle_proxy(args):
    print("=" * 65)
    print("  DART Network Channel Emulator Proxy")
    print("=" * 65)
    print(f"Listening:       {args.listen_host}:{args.listen_port}")
    print(f"Forwarding to:   {args.target_host}:{args.target_port}")
    print(f"Packet Loss:     {args.loss * 100:.1f}%")
    print(f"Delay Mean:      {args.delay:.1f} ms")
    print(f"Delay Jitter:    {args.jitter:.1f} ms")
    print(f"Corruption:      {args.corrupt * 100:.1f}%")
    print(f"Duplication:     {args.duplicate * 100:.1f}%")
    print(f"Reordering:      {args.reorder * 100:.1f}%")
    print(f"PRNG Seed:       {args.seed if args.seed is not None else 'Random'}")
    print("-" * 65)

    config = EmulatorConfig(
        loss_rate=args.loss,
        delay_mean_ms=args.delay,
        delay_jitter_ms=args.jitter,
        corruption_rate=args.corrupt,
        duplication_rate=args.duplicate,
        reorder_rate=args.reorder,
        seed=args.seed
    )

    proxy = ChannelProxy(
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        target_host=args.target_host,
        target_port=args.target_port,
        forward_config=config
    )

    try:
        proxy.start()
        print("Proxy running. Press Ctrl+C to terminate.")
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nStopping channel proxy...")
    finally:
        proxy.stop()
        print("Proxy stopped.")


def handle_benchmark(args):
    from experiments.run_experiments import run_all_experiments
    run_all_experiments()


def handle_dashboard(args):
    from web.server import run_server
    print(f"Starting DART Web Dashboard on port {args.port}...")
    run_server(host=args.host, port=args.port)


def main():
    parser = argparse.ArgumentParser(
        prog="dart",
        description="DART: Dynamic Adaptive Reliable Transport Protocol Suite"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: send
    p_send = subparsers.add_parser("send", help="Send a file over reliable UDP")
    p_send.add_argument("file", help="Path of the file to transmit")
    p_send.add_argument("--host", default="127.0.0.1", help="Target receiver host (default: 127.0.0.1)")
    p_send.add_argument("--port", type=int, default=9000, help="Target receiver port (default: 9000)")
    p_send.add_argument("--arq", choices=["sw", "gbn", "sr"], default="sr", help="ARQ protocol: sw, gbn, sr (default: sr)")
    p_send.add_argument("--window", type=int, default=5, help="Window size for pipelined ARQs (default: 5)")
    p_send.add_argument("--rto", type=float, default=0.3, help="Initial RTO in seconds (default: 0.3)")
    p_send.add_argument("--adaptive", action="store_true", help="Enable DART Adaptive Congestion & Window Engine")
    p_send.set_defaults(func=handle_send)

    # Subcommand: recv
    p_recv = subparsers.add_parser("recv", help="Receive a file over reliable UDP")
    p_recv.add_argument("output", help="Path to save the received file")
    p_recv.add_argument("--host", default="0.0.0.0", help="Listen host interface (default: 0.0.0.0)")
    p_recv.add_argument("--port", type=int, default=9000, help="Listen port (default: 9000)")
    p_recv.add_argument("--arq", choices=["sw", "gbn", "sr"], default="sr", help="ARQ protocol: sw, gbn, sr (default: sr)")
    p_recv.add_argument("--window", type=int, default=5, help="Receiver buffer window size (default: 5)")
    p_recv.add_argument("--timeout", type=float, default=30.0, help="Socket listen timeout (seconds)")
    p_recv.set_defaults(func=handle_recv)

    # Subcommand: proxy
    p_proxy = subparsers.add_parser("proxy", help="Run the network channel emulator proxy")
    p_proxy.add_argument("--listen-host", default="127.0.0.1")
    p_proxy.add_argument("--listen-port", type=int, default=8500)
    p_proxy.add_argument("--target-host", default="127.0.0.1")
    p_proxy.add_argument("--target-port", type=int, default=9000)
    p_proxy.add_argument("--loss", type=float, default=0.0, help="Packet drop rate [0.0 - 1.0]")
    p_proxy.add_argument("--delay", type=float, default=0.0, help="Propagation delay in ms")
    p_proxy.add_argument("--jitter", type=float, default=0.0, help="Delay jitter in ms")
    p_proxy.add_argument("--corrupt", type=float, default=0.0, help="Bit corruption rate [0.0 - 1.0]")
    p_proxy.add_argument("--duplicate", type=float, default=0.0, help="Packet duplication rate [0.0 - 1.0]")
    p_proxy.add_argument("--reorder", type=float, default=0.0, help="Packet reorder rate [0.0 - 1.0]")
    p_proxy.add_argument("--seed", type=int, default=None, help="PRNG seed for deterministic runs")
    p_proxy.set_defaults(func=handle_proxy)

    # Subcommand: benchmark
    p_bench = subparsers.add_parser("benchmark", help="Run automated P3 benchmark experiments & generate plots")
    p_bench.set_defaults(func=handle_benchmark)

    # Subcommand: dashboard
    p_dash = subparsers.add_parser("dashboard", help="Start the interactive Web Dashboard")
    p_dash.add_argument("--host", default="0.0.0.0")
    p_dash.add_argument("--port", type=int, default=8000)
    p_dash.set_defaults(func=handle_dashboard)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
