# AI-Use Disclosure & Implementation Record

**Student Name:** Areen Dubey  
**Roll Number:** 24051689  
**Section:** CSE 34  
**Course:** CS-30003 — Computer Networks (Autumn 2026)  
**Project:** DART — Dynamic Adaptive Reliable Transport (Catalogue Foundation: P3 Reliable Data Transfer over UDP + Adaptive Transmission Strategy)  
**Assigned Individual Ownership:** Overall Protocol Architecture, Wire Packet Codec, DART Adaptive Decision Engine, System Integration & Web Telemetry Server  
**Repository:** [github.com/areendubey/DART-Adaptive-UDP](https://github.com/areendubey/DART-Adaptive-UDP)  

---

## 1. Compliance Statement & Ground Rules

In strict adherence to the **Section 5 Ground Rules** outlined in the *CS-30003 Coding Assignment 1 Specification*:

1. **Permitted Scope:** Generative AI tools (Claude 3.5 Sonnet / OpenAI ChatGPT) were utilized selectively as interactive technical assistants for:
   - Clarifying networking RFC specifications and mathematical formulations (RFC 1071 internet checksum edge cases, RFC 6298 Jacobson/Karels EWMA smoothing).
   - Scaffolding Python dataclass/boilerplate definitions.
   - Brainstorming synthetic edge-case test vectors for packet decoding and state transitions.
   - Code review and static vulnerability identification.
2. **Prohibited Scope (Self-Implemented Core):** No core transport protocol, socket-level networking logic, ARQ loop, state machine transitions, or decision engine algorithms were generated and submitted as third-party work. All core architectural designs, socket I/O loops, packet validation boundaries, and adaptive state machine policies were designed, written, debugged, and integrated by **Areen Dubey**.
3. **Viva Accountability:** Every design choice, data structure, mathematical formula, bitmask operation, and state transition documented below was authored with complete theoretical understanding. I am fully prepared to modify, refactor, and defend any part of this code live during the individual viva.

---

## 2. Step-by-Step AI Interaction & Implementation Log

The following chronological records detail the specific instances where AI was queried, the exact prompts provided, the output returned, and the subsequent human analysis and core implementation.

---

### Step 1: Wire Packet Header Format & RFC 1071 Checksum Algorithm

#### 1.1 Architectural Objective
In accordance with catalogue P3 specifications, reliable UDP requires framing, sequence numbering, acknowledgements, flags, and integrity verification. I needed to design a binary header packed in network byte order (Big-Endian) that fits comfortably within standard Ethernet MTU (1500 bytes) without IP fragmentation, while implementing standard 16-bit One's Complement Internet Checksum (RFC 1071) from scratch at the bit level without third-party libraries.

#### 1.2 Prompt Given to AI
```text
I am designing a binary wire header for a custom Reliable UDP transport protocol (DART) in Python using standard `struct`. 
The packet must include:
- Magic identifier (2 bytes)
- Version (1 byte)
- Flags for DATA, ACK, SYN, FIN, RST (1 byte bitmask)
- 32-bit Sequence Number (4 bytes)
- 32-bit Acknowledgement Number (4 bytes)
- 16-bit Advertised Window (2 bytes)
- 16-bit Checksum (2 bytes)
- Variable payload (up to 1400 bytes MSS)

Can you show me the struct format string for big-endian packing and explain the RFC 1071 16-bit one's complement addition loop in Python, specifically how carry bit folding works if the payload has an odd byte length?
```

#### 1.3 AI Response Summary
The AI provided the format string `!H B B I I H H` (16 bytes total) and suggested a standard loop that iterated over pairs of bytes, summed them into a 32-bit integer, and folded carries:
```python
def internet_checksum(data):
    if len(data) % 2 != 0:
        data += b'\x00'
    total = sum((data[i] << 8) + data[i+1] for i in range(0, len(data), 2))
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)
    return ~total & 0xFFFF
```

#### 1.4 Critical Analysis & Core Logic Implemented by Areen
While the AI output provided the theoretical carry loop, it contained severe operational limitations for an active socket protocol:
- It did not account for zeroing out the checksum field during packet serialization before computing the checksum over the header + payload.
- It lacked validation against oversized payloads exceeding MSS, which would cause silent fragmentation or socket buffer overflow.
- It did not handle packet corruption detection or define an explicit domain exception for malformed bytes received over the raw UDP socket.

**My Core Implementation (`dart/packet.py`):**
1. I implemented `compute_internet_checksum(data: bytes)` with explicit byte-order handling and odd-byte zero padding (`data + b'\x00'`).
2. In `DARTPacket.serialize()`, I implemented the protocol integrity invariant: the packet is first packed with `checksum = 0`, the CRC/RFC 1071 checksum is calculated over `header_bytes + payload`, and then the header is repacked with the computed checksum.
3. In `DARTPacket.deserialize(data: bytes)`, I enforced strict size bounds (`HEADER_SIZE <= len(data) <= HEADER_SIZE + MAX_PAYLOAD_SIZE`), magic number verification (`0xDA87`), version matching, and re-computation of the checksum. If the recalculated checksum fails, a custom `PacketDecodeError("Checksum validation failed")` is raised and the datagram is cleanly dropped before reaching ARQ buffers.

```python
# Excerpt from dart/packet.py (Implemented by Areen)
def compute_internet_checksum(data: bytes) -> int:
    if len(data) % 2 != 0:
        data = data + b'\x00'

    total_sum = 0
    for i in range(0, len(data), 2):
        word = (data[i] << 8) + data[i + 1]
        total_sum += word
        while total_sum > 0xFFFF:
            total_sum = (total_sum & 0xFFFF) + (total_sum >> 16)

    return (~total_sum) & 0xFFFF
```

#### 1.5 Viva Defense & Live Modification Strategy
- **Questions Anticipated:** Why fold carries repeatedly (`while total_sum > 0xFFFF`) instead of a single fold? Why use One's Complement rather than a simple CRC-32 or additive sum?
- **Viva Defense:** One's complement addition allows end-to-end checksum verification independent of system endianness. The carry folding can produce a carry itself (e.g. `0xFFFF + carry = 0x10000`), requiring iterative or two-step folding.
- **Live Coding Demonstration:** Live during the viva, I can modify the checksum algorithm to introduce simulated 1-bit corruption in the wire stream to show `PacketDecodeError` discarding the packet.

---

### Step 2: DART Adaptive Engine & Finite State Machine Design

#### 2.1 Architectural Objective
The unique contribution of our project is the DART Adaptive Engine. Unlike static P3 transports (fixed window, static timeout), DART must dynamically modulate its transmission window (`min_window` to `max_window`) and packet pacing delay based on measured channel dynamics (RTT inflation, lost packets, timeouts). It must prevent congestion collapse while avoiding aggressive oscillation.

#### 2.2 Prompt Given to AI
```text
I am building the core adaptive decision engine for DART (Dynamic Adaptive Reliable Transport) in Python. 
I want to use an explainable Finite State Machine (STARTUP, STABLE, CONGESTION_DETECTED, RECOVERY) 
rather than an opaque reinforcement learning or complex heuristic.

Could you provide a Python dataclass structure representing engine configuration parameters 
(initial window, min/max bounds, decrease factor) and an Enum for the four states, 
along with suggestions on how to detect RTT inflation before actual loss occurs?
```

#### 2.3 AI Response Summary
The AI generated a basic `AdaptiveConfig` dataclass and recommended comparing smoothed RTT ($SRTT$) against the minimum observed baseline RTT ($RTT_{min}$):
$$\text{Inflated if } SRTT > RTT_{min} \times \alpha \quad (\text{suggesting } \alpha = 1.35)$$
However, the AI suggested an unbounded TCP Vegas-style equation that adjusted the window as a continuous floating-point number directly tied to raw packet counts.

#### 2.4 Critical Analysis & Core Logic Implemented by Areen
The AI's suggestion was inappropriate for a reliable ARQ transport because:
- Floating-point window sizing breaks discrete packet window boundaries in Go-Back-N and Selective Repeat.
- An unconstrained adaptation engine causes "flapping" (rapid back-and-forth window contraction and expansion) under momentary jitter.
- The AI's code lacked an isolation contract: adaptation must be purely deterministic and decoupled from socket I/O so it can be evaluated against a static baseline under identical seeds.

**My Core Implementation (`dart/adaptive/engine.py` & `dart_transport/adaptive.py`):**
1. **Architected the FSM:**
   - **`STARTUP`:** Bounded ramp-up (+1 packet per round-trip) until reaching an initial stable window of 8.
   - **`STABLE`:** Conservative Additive Increase (AIMD): increases transmission window by 1 only after 2 consecutive healthy RTT intervals without loss or inflation, while reducing pacing delay.
   - **`CONGESTION_DETECTED`:** Multiplicative Decrease: on any packet loss or timeout event, window is immediately reduced by `decrease_factor` ($0.5\times$ or $0.7\times$), clamped to `min_window = 1`, and a protective pacing delay (+2.0 ms) is injected to drain bottleneck router queues.
   - **`RECOVERY`:** Freezes window scaling and holds steady until `recovery_healthy_required = 3` consecutive clean observations are logged, preventing catastrophic multi-reduction on a single burst loss.
2. **RTT Smoothing & Early Queue Detection:** Implemented standard EWMA smoothing ($SRTT = 0.875 \cdot SRTT + 0.125 \cdot RTT_{sample}$) and checked against $min\_RTT \times 1.35$. If latency inflates without loss, pacing is throttled before drops occur.
3. **Pluggable Base Architecture:** Created `BaseAdaptiveEngine` and `FixedBaselineEngine` to ensure experimental fairness—both static P3 and adaptive DART share the identical socket and ARQ codebase.

```python
# Excerpt from dart/adaptive/engine.py (Authored by Areen)
class DARTAdaptiveEngine(BaseAdaptiveEngine):
    def on_rtt_sample(self, rtt: float) -> None:
        self.rtt_history.append(rtt)
        if len(self.rtt_history) > 50:
            self.rtt_history.pop(0)

        if self.min_rtt is None or rtt < self.min_rtt:
            self.min_rtt = rtt

        if self.srtt is None:
            self.srtt = rtt
        else:
            self.srtt = 0.875 * self.srtt + 0.125 * rtt

        # Delay-based early congestion warning (bufferbloat mitigation)
        if self.min_rtt and self.srtt > self.min_rtt * self.config.rtt_inflation_threshold:
            self.pacing_ms = min(self.config.max_pacing_ms, self.pacing_ms + 1.0)
            return

        if self.state == AdaptiveState.STABLE:
            self.healthy_observations += 1
            if self.healthy_observations >= 2:
                self.window = min(self.config.max_window, self.window + self.config.window_increase_step)
                self.pacing_ms = max(0.0, self.pacing_ms - 0.5)
                self.healthy_observations = 0
```

#### 2.5 Viva Defense & Live Modification Strategy
- **Questions Anticipated:** Why did you choose AIMD instead of MIMD? How do you prevent a single dropped ACK from causing repeated window drops?
- **Viva Defense:** Additive Increase Multiplicative Decrease (AIMD) is mathematically proven to converge to efficiency and fairness. Repeated drops from a single congestion episode are prevented by transitioning immediately to `RECOVERY`, where the window is locked for $N$ healthy samples.
- **Live Coding Demonstration:** In the viva, I can alter `decrease_factor` from `0.5` to `0.8` or adjust `rtt_inflation_threshold` to demonstrate on-the-fly tuning and observe real-time throughput changes.

---

### Step 3: Reliability Integration Contract & Event Boundaries

#### 3.1 Architectural Objective
As the Architecture Lead, I was responsible for defining how the ARQ implementations (Stop-and-Wait developed by Kartikey, Go-Back-N by Anushka, and Selective Repeat by Savvy) would interface with my Adaptive Engine without creating tight coupling or spaghetti code.

#### 3.2 Prompt Given to AI
```text
I am designing an integration layer between three pluggable ARQ protocols (Stop-and-Wait, GBN, SR) 
and an adaptive rate/window controller in Python. 
What design pattern best keeps the ARQ protocol loops completely isolated from the telemetry/metrics calculations, 
so that each ARQ protocol only calls clean lifecycle hooks?
```

#### 3.3 AI Response Summary
The AI recommended an Observer or Mediator pattern with an event-driven `ReliabilityController` exposing callbacks: `on_packet_sent`, `on_acknowledged`, `on_timeout`, and `evaluate`.

#### 3.4 Critical Analysis & Core Logic Implemented by Areen
The concept of callback hooks was solid, but the AI's naive implementation attempted to pass raw sockets into the controller, violating separation of concerns.

**My Core Implementation (`dart_transport/integration.py` & `docs/integration-contract.md`):**
1. I specified a strict unidirectional contract documented in `docs/integration-contract.md`: the ARQ sender owns the socket and transmission buffer; the `ReliabilityController` is purely analytical.
2. Formulated strict event hooks:
   - `on_packet_sent(sequence, sent_at, retransmission=False)`: records original vs retransmitted timestamps for Karn's algorithm.
   - `on_acknowledged(sequence, acked_at, bytes)`: feeds genuine RTT samples into the estimator.
   - `on_timeout()`: logs timeout events and returns backoff RTO.
   - `evaluate(now)`: queries the adaptive engine and returns a immutable `AdaptiveDecision(window, rate, action, reason)`.
3. Created `TransportMode.P3_BASELINE` vs `TransportMode.DART`: in P3 mode, the controller returns the fixed user-specified window; in DART mode, it delegates to the FSM. This enabled our exact scientific evaluation claim.

---

### Step 4: Unit Testing & Boundary Edge-Case Generation

#### 4.1 Architectural Objective
To satisfy Section 1 ("Code that runs but cannot be explained scores zero; constructing tests that prove correctness rather than just showing one successful transfer"), I needed exhaustive unit tests verifying packet corruption, out-of-order sequence boundaries, and FSM transitions under simulated severe loss.

#### 4.2 Prompt Given to AI
```text
I am writing Python unittest suites for my DART packet codec and adaptive state machine. 
Can you generate test fixtures for:
1. Verifying that corrupting any single bit in a 16-byte DART header triggers a checksum failure.
2. Simulating a sequence of metrics snapshots (10 packets healthy -> 2 packets lost -> healthy recovery) 
   to verify that the FSM transitions: STARTUP -> STABLE -> CONGESTION_DETECTED -> RECOVERY -> STABLE.
```

#### 4.3 AI Response Summary
The AI provided boilerplate `unittest.TestCase` structures with sample loops mutating single bytes in a bytearray and verifying `assertRaises(PacketDecodeError)`.

#### 4.4 Critical Analysis & Core Logic Implemented by Areen
The AI's generated mock metrics were partially detached from DART's actual data classes (`TransferMetrics` and `RttSnapshot`). 

**My Core Implementation (`tests/test_packet.py` & `tests/test_adaptive.py`):**
1. Refactored the test suite to use DART's actual internal structures (`dart_transport.metrics.TransferMetrics`, `RttSnapshot`, and `AdaptiveConfig`).
2. Implemented `test_minimum_bounds_prevent_over_aggressive_decrease()` to verify that under catastrophic 100% loss, the window is clamped at `min_window = 1` rather than collapsing to zero or throwing a `ZeroDivisionError`.
3. Validated end-to-end checksum verification with 37 automated tests passing cleanly across the repository.

```python
# Excerpt from tests/test_adaptive.py (Refactored and maintained by Areen)
class AdaptiveTests(unittest.TestCase):
    def test_loss_transitions_to_congestion_and_decreases_once_before_recovery(self) -> None:
        engine = AdaptiveEngine()
        engine.observe(_metrics())

        congested = engine.observe(_metrics(lost=2))
        recovery = engine.observe(_metrics())

        self.assertIs(congested.state, AdaptiveState.CONGESTION_DETECTED)
        self.assertIs(congested.action, AdaptationAction.DECREASE)
        self.assertIs(recovery.state, AdaptiveState.RECOVERY)
        self.assertIs(recovery.action, AdaptationAction.HOLD)
```

---

### Step 5: Web Monitoring Dashboard & Real-Time Telemetry API

#### 5.1 Architectural Objective
To provide a live visual proof of the adaptive state machine during the viva demo, I built a zero-dependency web dashboard showing real-time window adjustments, smoothed RTT graphs, and file transfer progress.

#### 5.2 Prompt Given to AI
```text
I need a minimal Python HTTP server using only the standard library (`http.server`) 
that can serve static HTML/JS files from a directory and expose a REST endpoint `/api/status` 
returning JSON telemetry from a background worker thread.
Show the basic boilerplate for `do_GET` routing.
```

#### 5.3 AI Response Summary
The AI provided boilerplate code subclassing `http.server.BaseHTTPRequestHandler` with an `if path == '/api/status':` condition returning JSON via `wfile.write`.

#### 5.4 Critical Analysis & Core Logic Implemented by Areen
The boilerplate was synchronous and lacked threading safety, which would cause the HTTP server to lock up if a client polled while a file transfer was executing.

**My Core Implementation (`web/server.py` & `web/app.js`):**
1. Built a multi-threaded HTTP server utilizing `socketserver.ThreadingMixIn` and `BaseHTTPRequestHandler`.
2. Implemented REST API routes:
   - `/api/status`: thread-safe telemetry extraction from `DARTAdaptiveEngine.telemetry_snapshot()`.
   - `/api/transfer/start`: initiates asynchronous background transfer tasks via Python's `threading.Thread`.
   - `/api/plots`: serves dynamically generated matplotlib benchmark graphs.
3. Designed the frontend telemetry polling loop in vanilla JavaScript (`web/app.js`), updating SVG gauge meters and real-time state tags (`STARTUP`, `STABLE`, `CONGESTION_DETECTED`, `RECOVERY`).

---

## 3. Summary of AI Tool Usage by Category

| Category | AI Utilization Level | Specific Purpose & AI Contribution | Human Engineering & Core Logic by Areen |
|---|:---:|---|---|
| **RFC & Concepts** | Medium | Querying RFC 1071 carry-folding edge cases & TCP delay-based congestion theory. | Formulated wire header specification, big-endian format `!H B B I I H H`, and MTU constraints. |
| **Boilerplate & Scaffolding** | Medium | Dataclass definitions, enum declarations, and `http.server` dispatch scaffolding. | Built state machine logic, thread synchronization, and metric aggregation. |
| **Code Review & Debugging** | Low | Verifying bitwise one's complement inversion and identifying potential carry overflow. | Implemented header zeroing on encode, checksum validation, and `PacketDecodeError` handling. |
| **Test Case Fixtures** | Medium | Generating parameterized arrays for byte corruption testing. | Integrated real `TransferMetrics` objects, deterministic loss injection, and boundary assertions. |
| **Core Protocol & Logic** | **None (0%)** | **Strictly prohibited by assignment rules.** | **100% authored by Areen:** FSM transitions, AIMD window modulation, RTT smoothing, packet parsing, and socket integration. |

---

## 4. Individual Viva Readiness & Code Modification Plan

The course instructor explicitly noted:
> *"The viva is the real check, and it is not adversarial — it is a conversation about your own system. If you built it, you will find it easy... I will ask you to modify your code live during the demo."*

To demonstrate direct authorship and deep command of the codebase, I have prepared for the following live code modifications:

1. **Live Checksum Modification (`dart/packet.py`):**
   - Can immediately alter `compute_internet_checksum` to invert the verification condition or corrupt the header on transmit to demonstrate instant packet rejection and retransmission triggers.
2. **Dynamic Adaptation Tuning (`dart/adaptive/engine.py`):**
   - Can modify `rtt_inflation_threshold` (e.g. from `1.35` to `1.10`) live to demonstrate how a more sensitive threshold aggressively paces down packets before the channel emulator drops them.
   - Can alter `decrease_factor` from `0.5` (halving) to `0.25` (severe reduction) and showcase the resulting drop on the web dashboard telemetry graph.
3. **Pluggable ARQ Swap (`dart/cli.py`):**
   - Can demonstrate swapping between Stop-and-Wait, Go-Back-N, and Selective Repeat under the identical adaptive engine with a single CLI flag `--arq [sw|gbn|sr] --adaptive`.

---

## 5. Formal Declaration

I hereby confirm that this record represents an honest and thorough disclosure of all AI assistance used in the development of my assigned components for **DART**. All core algorithms, state transition mechanisms, packet serialization, and architectural integrations are my own original work.

**Signed:**  
*Areen Dubey*  
Roll No: 24051689  
Department of Computer Science & Engineering  
Kalinga Institute of Industrial Technology (KIIT)
