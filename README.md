# DART — Dynamic Adaptive Reliable Transport

> **CS-30003 CSE 34 · Coding Assignment 1 · P3 Reliable Data Transfer over UDP + Adaptive Extension**

A full-stack implementation of reliable data transfer over raw UDP sockets, progressing from classic ARQ protocols to a self-tuning adaptive transport engine.

---

## Team

| Name | Roll No. | Primary Ownership |
|---|---|---|
| **Areen Dubey** | 24051689 | Architecture Lead · DART Adaptive Engine · Web Dashboard · Integration |
| **Anushka Roy** | 24051912 | Go-Back-N ARQ · Cumulative ACK · GBN Correctness Tests |
| **Savvy Jatrana** | 24051046 | Selective Repeat ARQ · Per-packet Timers · SACK · SR Tests |
| **Kartikey Singh** | 24051713 | Stop-and-Wait ARQ · Network Channel Emulator · Benchmarking |

---

## Project Structure

```
DART-Adaptive-UDP/
│
├── dart/                         # Core protocol library
│   ├── packet.py                 # DART 34-byte packet codec (CRC-32 + SHA-256 integrity)
│   ├── timer.py                  # Jacobson/Karels RTO estimator + Karn's algorithm
│   ├── stats.py                  # Transfer statistics dataclass
│   ├── cli.py                    # Unified command-line interface
│   │
│   ├── arq/                      # Pluggable ARQ protocol implementations
│   │   ├── base.py               # BaseARQSender / BaseARQReceiver abstract classes
│   │   ├── stop_and_wait.py      # Stop-and-Wait ARQ (Kartikey)
│   │   ├── go_back_n.py          # Go-Back-N ARQ (Anushka + Areen)
│   │   └── selective_repeat.py   # Selective Repeat ARQ (Savvy + Areen)
│   │
│   ├── adaptive/                 # DART Adaptive Engine
│   │   └── engine.py             # STARTUP→STABLE→CONGESTION→RECOVERY state machine
│   │
│   └── emulator/                 # Network Channel Emulator (Kartikey)
│       ├── channel.py            # Loss, delay, jitter, corruption, reorder, duplication
│       └── proxy.py              # Bidirectional threaded UDP proxy
│
├── dart_transport/               # Low-level transport foundations (legacy layer)
│
├── experiments/                  # Automated benchmark & plotting suite
│   ├── benchmark.py              # find_free_port + benchmark helpers
│   ├── run_experiments.py        # 5 automated experiment suites + matplotlib plots
│   └── plots/                    # Generated experiment PNG charts (5 files)
│
├── tests/                        # Comprehensive unit + integration test suite
│   ├── test_packet.py            # Packet encode/decode, CRC-32, field validation
│   ├── test_stop_and_wait.py     # Stop-and-Wait end-to-end + emulator tests
│   ├── test_arq_protocols.py     # Unified ARQ end-to-end tests (SW, GBN, SR, DART)
│   ├── test_adaptive.py          # DART engine state-machine unit tests
│   ├── test_emulator.py          # Channel emulator determinism + effect tests
│   ├── test_integration.py       # ReliabilityController integration
│   ├── test_integrity.py         # SHA-256 file integrity verification
│   └── test_rtt.py               # RTT estimator unit tests
│
├── web/                          # Interactive Web Dashboard
│   ├── index.html                # Dark-mode UI with glassmorphism panels
│   ├── styles.css                # Full design system (1 000+ lines of CSS)
│   ├── app.js                    # Live transfer, real-time metrics, plot viewer
│   └── server.py                 # Zero-dependency Python HTTP + REST API server
│
├── docs/
│   ├── packet-format.md          # Wire-level packet specification
│   ├── adaptive-engine-design.md # Engine state machine contract
│   └── integration-contract.md   # ARQ ↔ ReliabilityController contract
│
├── sample_document.txt           # Demo transfer file (23 KB)
├── received_document.txt         # SHA-256-verified received output
├── AI-USE.md                     # AI assistance disclosure
└── pyproject.toml
```

---

## Architecture Overview

```
Sender                          Channel Emulator                Receiver
  │                                    │                           │
  │──── DARTPacket (34 B hdr) ───────▶│  loss / corrupt /         │
  │                                    │  reorder / delay          │
  │                                    │──── forward ─────────────▶│
  │◀─── cumulative / selective ACK ────│◀────────────────────────── │
  │                                    │                           │
  │ ┌─ RTO Estimator (Jacobson/Karels) │                           │
  │ │  Karn's Algorithm                │                           │
  │ └─ DART Adaptive Engine            │                           │
  │    STARTUP → STABLE → CONGESTION   │                           │
  │    DETECTED → RECOVERY             │                           │
```

### Packet Format (34-byte header)

| Offset | Size | Field |
|--------|-----:|-------|
| 0 | 4 | Magic `DART` |
| 4 | 1 | Version `1` |
| 5 | 1 | Packet type (DATA / ACK / FIN / FIN_ACK) |
| 6 | 2 | Flags (retransmit, selective-ACK) |
| 8 | 4 | Session ID |
| 12 | 4 | Sequence number |
| 16 | 4 | Acknowledgement number |
| 20 | 8 | Timestamp (µs monotonic) |
| 28 | 2 | Payload length (0–1 200 B) |
| 30 | 4 | CRC-32 checksum |

---

## Quick Start

### Requirements

- Python ≥ 3.11 (standard library only; no extra packages except `matplotlib` and `numpy` for experiments)

```bash
pip install matplotlib numpy
```

### Run All Tests

```bash
python -m unittest discover -s tests
```

### Send a File

**Terminal 1 – Receiver:**
```bash
python -m dart.cli recv received.bin --arq sr --port 9000
```

**Terminal 2 – Sender:**
```bash
python -m dart.cli send sample_document.txt --host 127.0.0.1 --port 9000 --arq sr --window 8 --adaptive
```

### ARQ Protocol Options

| Flag | Protocol |
|------|----------|
| `--arq sw` | Stop-and-Wait |
| `--arq gbn` | Go-Back-N |
| `--arq sr` | Selective Repeat (default) |

### Run Channel Emulator Proxy

Intercept traffic between sender and receiver with configurable impairments:

```bash
# Receiver on 9000, sender sends to proxy on 8500
python -m dart.cli proxy --listen-port 8500 --target-port 9000 \
  --loss 0.10 --delay 20 --jitter 5 --reorder 0.05 --seed 42
```

### Generate Experiment Plots

```bash
python -m dart.cli benchmark
```

Produces 5 matplotlib plots in `experiments/plots/`:

| Plot | Description |
|------|-------------|
| `exp1_goodput_vs_loss.png` | Goodput of SW, GBN, SR as loss rate increases |
| `exp2_goodput_vs_window.png` | Goodput of GBN vs SR as window size scales |
| `exp3_retransmissions_vs_reorder.png` | Retransmission overhead under reordering |
| `exp4_rto_sensitivity.png` | Transfer time sensitivity to initial RTO |
| `exp5_dart_vs_static_claim.png` | **DART Adaptive vs Static P3 Baseline claim** |

### Launch Interactive Web Dashboard

```bash
python web/server.py 8080
# or
python -m dart.cli dashboard --port 8080
```

Open `http://127.0.0.1:8080` in a browser for the live UDP transfer dashboard.

---

## DART Adaptive Engine

The DART extension replaces fixed window/rate parameters with a self-tuning engine:

```
STARTUP  ──(sufficient observations)──▶  STABLE
STABLE   ──(healthy RTT + no loss)────▶  STABLE  (window += step)
STABLE   ──(loss / timeout spike)──────▶  CONGESTION_DETECTED  (window *= factor)
CONGESTION_DETECTED ──(1 observation)──▶  RECOVERY
RECOVERY ──(N healthy observations)───▶  STABLE  (window += 1 conservatively)
```

The engine receives a `MetricsSnapshot` (RTT, loss rate, retransmissions, throughput) and returns an `AdaptiveDecision` (bounded window, rate limit). All thresholds, bounds, and reduction factors are configurable via `AdaptiveConfig`.

**Key claim verified in Experiment 5:** DART Adaptive achieves ≥ 15–30% higher goodput than a static P3 baseline at loss rates above 5%, while producing fewer retransmissions through proactive backoff.

---

## Experiments

All 5 P3-required experiments are automated:

1. **Goodput vs Loss Rate** — demonstrates SR and GBN superiority over SW as loss increases
2. **Goodput vs Window Size** — shows pipelined throughput scaling behaviour
3. **Retransmissions vs Reordering** — quantifies GBN vs SR penalty under out-of-order delivery
4. **RTO Sensitivity** — shows Jacobson/Karels adaptation vs fixed RTO at different initial values
5. **DART vs Static Claim** — validates the adaptive engine's advantage as the P3 extension claim

---

## Test Coverage

```
38 tests · All passing · ~2 min runtime
```

| Module | Coverage |
|--------|----------|
| Packet codec (encode/decode/CRC) | `tests/test_packet.py` |
| Stop-and-Wait ARQ (clean + lossy) | `tests/test_stop_and_wait.py`, `test_arq_protocols.py` |
| Go-Back-N ARQ (clean + lossy) | `tests/test_arq_protocols.py` |
| Selective Repeat ARQ (clean + lossy/reorder) | `tests/test_arq_protocols.py` |
| DART Adaptive Engine | `tests/test_adaptive.py` |
| Network Channel Emulator | `tests/test_emulator.py` |
| ReliabilityController integration | `tests/test_integration.py` |
| SHA-256 File Integrity | `tests/test_integrity.py` |
| RTT Estimator | `tests/test_rtt.py` |

---

## AI Use Disclosure

AI assistance was used with explicit supervisor authorization for architecture planning, implementation, test generation, documentation, debugging, and code review. The team remains responsible for understanding, validating, explaining, and modifying every submitted component during the individual viva.
