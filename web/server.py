"""
DART Interactive Web Dashboard Server
Author: Areen Dubey (Architecture & Integration Lead)
Course: CS-30003 CSE 34 | Catalogue Foundation: P3 Reliable Data Transfer over UDP

A lightweight, zero-dependency HTTP API and static file server built directly on
Python standard library modules (http.server, socket, threading, json, hashlib).
Serves the dynamic Web UI and coordinates live UDP transfers through the Channel Emulator.
"""

import os
import sys
import json
import time
import socket
import threading
import tempfile
import hashlib
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dart.arq import get_sender, get_receiver, ARQType
from dart.emulator.channel import EmulatorConfig
from dart.emulator.proxy import ChannelProxy
from dart.adaptive.engine import DARTAdaptiveEngine, FixedBaselineEngine
from experiments.benchmark import find_free_port

WEB_DIR = os.path.dirname(os.path.abspath(__file__))
PLOTS_DIR = os.path.join(REPO_ROOT, "experiments", "plots")


class DARTDashboardHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler for DART Web UI and REST API."""

    def log_message(self, format, *args):
        # Clean terminal logging
        sys.stdout.write(f"[{self.log_date_time_string()}] {self.address_string()} - {format % args}\n")

    def do_GET(self):
        url_path = self.path.split("?")[0]

        if url_path == "/" or url_path == "/index.html":
            self._serve_file(os.path.join(WEB_DIR, "index.html"), "text/html")
        elif url_path == "/styles.css":
            self._serve_file(os.path.join(WEB_DIR, "styles.css"), "text/css")
        elif url_path == "/app.js":
            self._serve_file(os.path.join(WEB_DIR, "app.js"), "application/javascript")
        elif url_path.startswith("/api/plots/"):
            filename = os.path.basename(url_path)
            file_path = os.path.join(PLOTS_DIR, filename)
            if os.path.exists(file_path):
                self._serve_file(file_path, "image/png")
            else:
                self.send_error(404, f"Plot {filename} not yet generated or not found.")
        elif url_path == "/api/status":
            self._send_json({
                "status": "online",
                "kernel": "Raw UDP Sockets (AF_INET, SOCK_DGRAM)",
                "protocols": ["stop_and_wait", "go_back_n", "selective_repeat"],
                "modes": ["p3_static", "dart_adaptive"],
                "version": "1.0.0"
            })
        else:
            self.send_error(404, "Endpoint not found")

    def do_POST(self):
        url_path = self.path.split("?")[0]

        if url_path == "/api/transfer":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            try:
                params = json.loads(body.decode("utf-8"))
            except Exception as e:
                self.send_error(400, f"Malformed JSON: {e}")
                return

            result = self._execute_live_transfer(params)
            self._send_json(result)
        else:
            self.send_error(404, "Endpoint not found")

    def _serve_file(self, filepath: str, content_type: str):
        if not os.path.exists(filepath):
            self.send_error(404, "File not found")
            return

        with open(filepath, "rb") as f:
            content = f.read()

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(content)

    def _send_json(self, data: Dict[str, Any]):
        encoded = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(encoded)

    def _execute_live_transfer(self, p: Dict[str, Any]) -> Dict[str, Any]:
        """Runs a live transfer using the chosen ARQ and Channel Emulator parameters."""
        arq_str = p.get("arq_type", "selective_repeat")
        mode_str = p.get("mode", "dart")
        payload_kb = int(p.get("payload_kb", 50))
        loss_rate = float(p.get("loss_rate", 0.10))
        delay_ms = float(p.get("delay_ms", 15.0))
        jitter_ms = float(p.get("jitter_ms", 5.0))
        reorder_rate = float(p.get("reorder_rate", 0.08))
        corrupt_rate = float(p.get("corrupt_rate", 0.0))
        dup_rate = float(p.get("dup_rate", 0.0))
        seed = int(p.get("seed", 42))

        # Generate payload bytes
        data = os.urandom(payload_kb * 1024)
        src_sha = hashlib.sha256(data).hexdigest()

        with tempfile.NamedTemporaryFile(delete=False) as f_src, tempfile.NamedTemporaryFile(delete=False) as f_dst:
            src_path = f_src.name
            dst_path = f_dst.name
            f_src.write(data)
            f_src.flush()

        rx_port = find_free_port()
        proxy_port = find_free_port()

        config = EmulatorConfig(
            loss_rate=loss_rate,
            delay_mean_ms=delay_ms,
            delay_jitter_ms=jitter_ms,
            corruption_rate=corrupt_rate,
            reorder_rate=reorder_rate,
            duplication_rate=dup_rate,
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

        # Instantiate ARQ Receiver
        rx = get_receiver(arq_str, listen_host="127.0.0.1", listen_port=rx_port)

        # Setup Engine
        is_adaptive = (mode_str == "dart")
        engine = DARTAdaptiveEngine() if is_adaptive else FixedBaselineEngine(fixed_window=4)

        tx_kwargs = {
            "initial_rto": 0.3,
            "min_rto": 0.05,
            "max_rto": 4.0
        }
        if arq_str != "stop_and_wait":
            tx_kwargs["window_size"] = 4
            tx_kwargs["adaptive_engine"] = engine

        tx = get_sender(arq_str, host="127.0.0.1", port=proxy_port, **tx_kwargs)

        rx_thread = threading.Thread(target=lambda: rx.receive_file(dst_path, timeout_seconds=25.0), daemon=True)

        try:
            rx_thread.start()
            time.sleep(0.04)

            tx_stats = tx.send_file(src_path)
            rx_thread.join(timeout=10.0)

            with open(dst_path, "rb") as f:
                dst_data = f.read()

            dst_sha = hashlib.sha256(dst_data).hexdigest()
            integrity_ok = (src_sha == dst_sha)

            return {
                "success": True,
                "elapsed_s": tx_stats.elapsed_time,
                "goodput_kb_s": tx_stats.goodput_kb_s,
                "goodput_mbps": tx_stats.goodput_mbps,
                "overhead_pct": tx_stats.retransmission_overhead_pct,
                "total_packets": tx_stats.total_packets_sent,
                "retransmissions": tx_stats.total_packets_retransmitted,
                "timeouts": tx_stats.total_timeouts,
                "avg_rtt_ms": tx_stats.avg_rtt_ms,
                "current_rto_ms": (tx.rto_estimator.current_rto * 1000.0) if hasattr(tx, "rto_estimator") else 300.0,
                "source_sha256": src_sha,
                "received_sha256": dst_sha,
                "integrity_ok": integrity_ok,
                "total_chunks": max(1, payload_kb * 1024 // 1400),
                "adaptive_state": engine.get_state() if hasattr(engine, "get_state") else "STATIC_P3"
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "elapsed_s": 0.0,
                "goodput_kb_s": 0.0,
                "goodput_mbps": 0.0,
                "overhead_pct": 0.0,
                "total_packets": 0,
                "retransmissions": 0,
                "timeouts": 0,
                "avg_rtt_ms": 0.0,
                "current_rto_ms": 300.0,
                "source_sha256": src_sha,
                "received_sha256": "Error during reception",
                "integrity_ok": False,
                "total_chunks": max(1, payload_kb * 1024 // 1400),
                "adaptive_state": "ERROR"
            }

        finally:
            proxy.stop()
            for pth in (src_path, dst_path):
                if os.path.exists(pth):
                    os.remove(pth)


def run_server(host: str = "127.0.0.1", port: int = 8080):
    server_address = (host, port)
    httpd = HTTPServer(server_address, DARTDashboardHandler)
    print("=" * 65)
    print(f"[*] DART Interactive Web Dashboard Server is Live!")
    print(f"[*] Local URL: http://{host}:{port}")
    print(f"    Architecture: Sockets-Only UDP Transport | P3 + Adaptive Engine")
    print("=" * 65)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")
        httpd.server_close()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    run_server(port=port)

