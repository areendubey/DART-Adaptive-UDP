# AI-Use Disclosure & Implementation Record

**Student Name:** Anushka Roy  
**Roll Number:** 24051912  
**Section:** CSE 34  
**Course:** CS-30003 — Computer Networks (Autumn 2026)  
**Project:** DART — Dynamic Adaptive Reliable Transport (Catalogue Foundation: P3 Reliable Data Transfer over UDP + Adaptive Transmission Strategy)  
**Assigned Individual Ownership:** Go-Back-N ARQ, Cumulative ACK & Retransmission Handling, Reliability Testing & Protocol Correctness Validation, GBN Integration with the shared DART transport  
**Repository:** [github.com/areendubey/DART-Adaptive-UDP](https://github.com/areendubey/DART-Adaptive-UDP)  
**Reference Material:** `CN2.zip` — contains `gbn_integration.patch` (my GBN integration changes) and `DART-Adaptive-UDP-updated.zip` (the updated repository with `dart/`, `tests/` and `docs/INTEGRATION.md`).  

---

## 1. Compliance Statement & Ground Rules

In strict adherence to the **Section 5 Ground Rules** outlined in the *CS-30003 Coding Assignment 1 Specification*:

1. **Permitted Scope:** Generative AI (Claude) was used selectively as an interactive technical assistant for:
   - Clarifying RFC 6298 (Jacobson/Karels RTO smoothing) and Karn's algorithm.
   - Applying the changes requested in the team review notes to my GBN module: dynamic RTO, unified packet header, `ReliabilityController` hooks, and a swappable test channel.
   - Scaffolding the small stand-in classes (`dart/rtt.py`, `dart/controller.py`, `dart/compat.py`) used until Areen's `dart_transport` was available.
   - Generating test cases for the RTT estimator, Karn's rule and controller hooks, and diagnosing a slow loss test.
2. **Prohibited Scope (Self-Implemented Core):** The Go-Back-N protocol itself was designed and written by me: the sliding window (`_base`, `_nextseq`), the single retransmission timer, cumulative ACK handling (`_handle_cumulative_ack`), whole-window retransmission (`_retransmit_window`), the in-order receiver (`GoBackNReceiver`), serial-number comparison (`seq_lt`, `seq_le`, `seq_add`), and my original reliability tests. The integration changes in `gbn_integration.patch` were AI-assisted; I read, ran and checked each one against the test suite.
3. **Viva Accountability:** Every design choice, formula and state change documented below is something I can explain. I am prepared to modify, refactor, and defend any part of this code live during the individual viva.

---

## 2. Step-by-Step AI Interaction & Implementation Log

The following records show where AI was queried, the prompt given, the output returned, and the analysis and implementation I did afterwards. All file names refer to the contents of `CN2.zip`.

---

### Step 1: Dynamic RTO (Jacobson/Karels) and Karn's Algorithm

#### 1.1 Architectural Objective
The review notes said my fixed 0.35 s timeout was not adaptive. Go-Back-N needed a retransmission timeout that follows the measured RTT, and RTT samples must never be taken from retransmitted packets (Karn's algorithm).

#### 1.2 Prompt Given to AI
```text
Change my Go-Back-N sender to use a dynamic retransmission timeout (Jacobson/Karels, RFC 6298)
instead of the fixed timeout, and follow Karn's algorithm.
```

#### 1.3 AI Response Summary
The AI proposed an `RttEstimator` holding SRTT and RTTVAR with RTO = SRTT + 4 × RTTVAR, clamped between a minimum and maximum, doubling the RTO on each timeout, and skipping RTT samples from retransmitted packets.

#### 1.4 Critical Analysis & Core Logic Implemented by Anushka
The AI output described the estimator but did not say how it fits a Go-Back-N sender, which resends the **whole window** on a timeout. I checked how this affects sampling and the timer.

**My Core Implementation (`dart/rtt.py`, `dart/gbn.py`, `dart/constants.py`):**
1. `RttEstimator` stores SRTT and RTTVAR (ALPHA = 0.125, BETA = 0.25, K = 4) and clamps RTO to `RTO_MIN_S = 0.1` and `RTO_MAX_S = 2.0`. The sender argument `timeout=` is now only the **initial** RTO (`DEFAULT_TIMEOUT_S = 0.35`).
2. **Karn part 1 (no samples from resent packets):** every sequence number resent on a timeout is added to `_retransmitted`. An ACK for such a sequence number is counted in `stats.rtt_samples_skipped` and never given to the estimator.
3. **Karn part 2 (backoff):** each timeout calls `estimator.on_timeout()`, which doubles the RTO up to the maximum.
4. **Single deadline-based timer:** `_timer_deadline = now + estimator.rto`. It restarts when the window moves and stops when everything is acknowledged.
5. **Timestamp echo:** the sender stamps each DATA packet in microseconds, and the receiver copies the stamp into the ACK. RTT is `(now - echo_timestamp)`. Repeated ACKs carry timestamp 0, so they are never used for RTT.

```python
# Excerpt from dart/rtt.py (RFC 6298 update)
def update(self, sample):
    """Feed one RTT sample (seconds) from a packet that was NOT retransmitted."""
    if sample <= 0:
        return
    if self.srtt is None:
        self.srtt = sample
        self.rttvar = sample / 2.0
    else:
        difference = self.srtt - sample
        if difference < 0:
            difference = -difference
        self.rttvar = (1 - BETA) * self.rttvar + BETA * difference
        self.srtt = (1 - ALPHA) * self.srtt + ALPHA * sample
    self.samples = self.samples + 1
    self._rto = self._clamp(self.srtt + K * self.rttvar)
```

#### 1.5 Viva Defense & Live Modification Strategy
- **Questions Anticipated:** Why do we not sample retransmitted packets? Why double the RTO on a timeout? Why is `reset_backoff()` needed?
- **Viva Defense:** An ACK for a resent packet could belong to the first or the second copy, so its RTT is ambiguous (Karn). Doubling the RTO stops us from flooding a congested path. `reset_backoff()` is my own design choice: under heavy loss, GBN resends the whole window, so clean samples are rare. Without the reset the RTO stayed at the 2 s cap while the measured RTT was about 0.4 ms. After adding it, the 40%-loss test dropped from about 49 s to about 10 s.
- **Live Coding Demonstration:** In the viva I can change `RTO_MIN_S`, `RTO_MAX_S` or `K` in `dart/constants.py` / `dart/rtt.py` and rerun `tests/test_gbn_rtt_controller.py` to show how the timeout changes.

---

### Step 2: Unified DART Packet Header

#### 2.1 Architectural Objective
The team agreed on one shared header. My old 14-byte header with the 16-bit Internet checksum (`dart/checksum.py`) had to be replaced, and the new fields (flags, timestamp) had to carry what GBN needs.

#### 2.2 Prompt Given to AI
```text
Change my packet header to the unified DART header: magic, version, flags, seq, ack,
timestamp, payload length and CRC32. Keep my GBN code working with the new Packet class.
```

#### 2.3 AI Response Summary
The AI gave the 28-byte network-order format `!4sBBIIQHI` and `Packet.encode()` / `Packet.decode()` methods using `struct` and `zlib.crc32`.

#### 2.4 Critical Analysis & Core Logic Implemented by Anushka
**My Core Implementation (`dart/packet.py`, `dart/constants.py`, `tests/test_gbn_packet.py`):**
1. Header layout (28 bytes): `magic "DART"(4) | version(1) | flags(1) | seq(4) | ack(4) | timestamp_us(8) | payload_len(2) | crc32(4)`, followed by the payload (up to `MAX_PAYLOAD = 1400`).
2. Flags: `FLAG_DATA = 0x01`, `FLAG_ACK = 0x02`, `FLAG_FIN = 0x04`, mapped to `PacketType.DATA / ACK / FIN`.
3. The CRC32 covers the whole packet with the crc32 field set to zero. `Packet.decode()` raises `PacketError` for a bad magic number, truncated data, wrong length or CRC failure. The datagram is dropped before it reaches the GBN logic.
4. Rewrote `tests/test_gbn_packet.py` for the new layout. `dart/checksum.py` is left in place but no longer imported by GBN.

#### 2.5 Viva Defense & Live Modification Strategy
- **Questions Anticipated:** Why is the CRC computed with the crc32 field zeroed? Why put a timestamp in the header?
- **Viva Defense:** The receiver must be able to recompute the same value, so the field being calculated must hold a fixed value (zero). The timestamp lets the receiver echo the send time in the ACK, so the sender measures RTT without keeping a separate table.
- **Assumption to confirm:** the timestamp is in microseconds and the field order matches Areen's real `Packet`.

---

### Step 3: ReliabilityController Hooks (P3 Static and DART Adaptive Mode)

#### 3.1 Architectural Objective
Go-Back-N must run in two modes without changing its loop: **P3 Static** (fixed window) and **DART Adaptive** (window decided by Areen's controller).

#### 3.2 Prompt Given to AI
```text
Add ReliabilityController hooks (on_packet_sent, on_acknowledged, on_timeout, window_size)
to my GBN sender so the adaptive controller can decide the window size while a transfer runs.
```

#### 3.3 AI Response Summary
The AI created a base `ReliabilityController` and a `StaticController`, and showed where the sender should call each hook.

#### 3.4 Critical Analysis & Core Logic Implemented by Anushka
**My Core Implementation (`dart/controller.py`, `dart/gbn.py`):**
1. The sender reads `controller.window_size` before every send decision (`_wait_for_window_slot`).
2. Hooks: `on_packet_sent(seq, is_retransmit)`, `on_acknowledged(ack, newly_acked, rtt_sample)` (`rtt_sample` is `None` when Karn's rule skipped it) and `on_timeout()`.
3. If the window shrinks while packets are in flight, nothing is recalled. GBN waits until `outstanding < window`. `stats.min_window_seen` and `stats.max_window_seen` record how the window moved.
4. A window below 1 is treated as 1.
5. Default is `StaticController(window)`. Adaptive mode is `GoBackNSender(sock, dest, controller=adaptive_controller)`.

```python
# Excerpt from dart/gbn.py (_handle_cumulative_ack)
new_base = seq_add(ack)
...
reset = getattr(self.estimator, "reset_backoff", None)
if reset is not None:
    reset()
...
self.controller.on_acknowledged(ack, released, rtt_sample)
```

#### 3.5 Viva Defense & Live Modification Strategy
- **Questions Anticipated:** What happens if the window shrinks during a transfer? How does the same GBN code run in both modes?
- **Viva Defense:** GBN only talks to hooks and a `window_size` property, so it never needs to know how the window is chosen. A shrinking window only delays new sends; it never recalls packets already sent.
- **Live Coding Demonstration:** I can pass a custom controller (like `GrowShrinkController` in `tests/test_gbn_rtt_controller.py`) and show the window changing in `stats.min_window_seen / max_window_seen`.
- **Assumption to confirm:** hook names and argument order come from the review notes and must be checked against Areen's real controller.

---

### Step 4: Integration Layer & Team Channel Emulator Swap Point

#### 4.1 Architectural Objective
Areen's `dart_transport` package and the team channel emulator were not available when this branch was written. GBN had to be built so the real code can be plugged in later with minimal change.

#### 4.2 Prompt Given to AI
```text
Areen's dart_transport is not ready. How do I write GBN so I can switch to it later?
Also make my tests get the lossy socket from one function so I can replace it with the team emulator.
```

#### 4.3 AI Response Summary
The AI suggested one module that decides where the shared classes come from, and one function that returns the test channel.

#### 4.4 Critical Analysis & Core Logic Implemented by Anushka
**My Core Implementation (`dart/compat.py`, `tests/harness.py`, `docs/INTEGRATION.md`):**
1. `dart/compat.py` is the **only** place `gbn.py` imports `Packet`, `PacketError`, `PacketType`, `RttEstimator` and `ReliabilityController` from. Switching to `dart_transport` is a three-line change there.
2. `tests/harness.py::make_channel()` returns the lossy socket for every test. When the shared emulator is ready, return it from this one function and delete `tests/lossy_socket.py`. The emulator needs `sendto`, `recvfrom` and `settimeout`.
3. `docs/INTEGRATION.md` lists the exact interface GBN assumes for each piece, so Areen can check it against the real code.

#### 4.5 Viva Defense & Live Modification Strategy
- **Viva Defense:** `Packet`, `RttEstimator` and `ReliabilityController` in my repository are **stand-ins** written from the review notes, not Areen's real classes. This is stated in `docs/INTEGRATION.md`.

---

### Step 5: Unit Testing, Reliability Validation & Debugging

#### 5.1 Architectural Objective
To prove correctness, not just show one successful transfer, I needed tests for lost data, lost ACKs, corruption, out-of-order data, sequence-number order, the RTO estimator, Karn's rule and the controller hooks.

#### 5.2 Prompt Given to AI
```text
Write tests for the RTT estimator, Karn's rule and the controller hooks in my GBN sender.
Why is my 40% loss test so slow?
```

#### 5.3 AI Response Summary
The AI produced test cases for the estimator and hooks, and explained that the RTO stayed at its maximum after repeated timeouts.

#### 5.4 Critical Analysis & Core Logic Implemented by Anushka
**My Core Implementation (`tests/`):**
1. Original tests written by me: `test_gbn_reliability.py` (data loss, ACK loss, dropped ACKs causing window retransmit, corruption not delivered, high-loss seeded run, cumulative ACK past a prefix), `test_gbn_correctness.py` (sequence order, empty payload, single packet, multi-window file, window of 1, SHA-256 integrity) and `test_gbn_ack_handling.py` (out-of-order discard, cumulative ACK releases a whole prefix).
2. Read and ran all 27 AI-assisted tests in `test_gbn_rtt_controller.py` (estimator values, clamping, backoff, Karn skip, timer stop, duplicate ACK, timestamp echo, static and adaptive hook behaviour).
3. Fixed the slow loss test with `reset_backoff()` (see Step 1).
4. The full suite of **51 tests** passes with `python -m unittest discover -s tests -t .`.

---

## 3. Summary of AI Tool Usage by Category

| Category | AI Utilization Level | Specific Purpose & AI Contribution | Human Engineering & Core Logic by Anushka |
|---|:---:|---|---|
| **RFC & Concepts** | Medium | Explaining RFC 6298 and Karn's algorithm. | Chose how Karn's rule applies to GBN's whole-window resend, and the `reset_backoff()` design. |
| **Boilerplate & Scaffolding** | Medium | `rtt.py`, `controller.py`, `compat.py` and the unified header code. | Wired them into the sender, checked every field and hook, wrote `docs/INTEGRATION.md`. |
| **Code Review & Debugging** | Low | Finding why the 40%-loss test was slow. | Confirmed the cause and verified the fix by running the test. |
| **Test Case Fixtures** | Medium | `test_gbn_rtt_controller.py` and `make_channel()`. | Read, ran and understood each test. Wrote the original reliability, correctness and ACK tests. |
| **Core Protocol & Logic** | **None (0%)** | — | **Authored by Anushka:** sliding window, single timer, cumulative ACK, whole-window retransmission, in-order receiver, serial-number arithmetic. |

---

## 4. Individual Viva Readiness & Code Modification Plan

To demonstrate direct authorship and command of the code in `CN2.zip`, I have prepared for these live modifications:

1. **Window size (`dart/gbn.py`, CLI):**
   - Run the transfer with `--window 1` and `--window 8` and show the effect on `stats.retransmissions` and transfer time. Window 1 behaves like Stop-and-Wait.
2. **Timeout tuning (`dart/constants.py`, `dart/rtt.py`):**
   - Change `RTO_MIN_S`, `RTO_MAX_S` or `K` live, rerun `tests/test_gbn_rtt_controller.py` and explain the change.
3. **Karn's rule (`dart/gbn.py`):**
   - Remove the `_retransmitted` check in `_handle_cumulative_ack`, and show `test_karn_no_sample_from_retransmitted_packet` fail.
4. **Loss injection (`tests/lossy_socket.py`, `tests/harness.py`):**
   - Raise `drop_prob` and corruption probability and show GBN still delivers the correct SHA-256.
5. **Adaptive controller (`dart/controller.py`):**
   - Plug in a custom controller and show the window change while the transfer still completes.

---

## 5. Known Limitations

- `Packet`, `RttEstimator` and `ReliabilityController` are stand-ins and must be checked against Areen's real `dart_transport` (field order, timestamp unit, hook names).
- **Not fixed:** the receiver returns as soon as it accepts FIN. If the final ACK is lost, the sender keeps resending FIN with no answer. A short linger after FIN (re-ACK repeated FINs for about 1 s) would fix it. It was outside the four requested changes.

---

## 6. Formal Declaration

I hereby confirm that this record represents an honest and thorough disclosure of all AI assistance used in the development of my assigned components for **DART**. The core Go-Back-N protocol is my own original work. The integration changes in `gbn_integration.patch` were AI-assisted, and I have read, tested and understood each of them.

**Signed:**  
*Anushka Roy*  
Roll No: 24051912  
Department of Computer Science & Engineering  
Kalinga Institute of Industrial Technology (KIIT)
