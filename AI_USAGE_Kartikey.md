# AI-Use Disclosure & Implementation Record

**Student Name:** Kartikey Singh  
**Roll Number:** 24051713  
**Section:** CSE 34  
**Course:** CS-30003 — Computer Networks  
**Project:** DART — Dynamic Adaptive Reliable Transport  
**Catalogue Foundation:** P3 — Reliable Data Transfer over UDP + Adaptive Transmission Strategy  
**Assigned Individual Ownership:** Stop-and-Wait ARQ, channel emulator, experiment automation/logging, and performance benchmarking  
**Team Repository:** [github.com/areendubey/DART-Adaptive-UDP](https://github.com/areendubey/DART-Adaptive-UDP)

---

## 1. Compliance Statement & Ground Rules

This document records the use of ChatGPT as an interactive learning, planning, coding-assistance, and troubleshooting tool during work on DART. The entries below describe the main prompts and assistance relevant to my assigned components. Prompt wording is summarized for readability and is not represented as a verbatim export of every chat message.

1. **Purpose of AI assistance:** AI was used to help interpret the project proposal, break down the assigned work, explain networking concepts, suggest implementation structures and test scenarios, review code behaviour, and troubleshoot local execution.
2. **Human responsibility:** AI output is not treated as automatically correct. I am responsible for reading and understanding the implementation, checking it against the assignment specification and team interfaces, running the code, investigating failures, and explaining the design during the individual viva.
3. **Disclosure:** The project code and this record should be reviewed against the actual repository history and the instructor's AI-use policy before submission. This document does not claim that every line of code was independently authored without AI assistance.
4. **Validation:** A successful local run or a small quick benchmark is not proof that every network condition or edge case has been fully tested. Results should be described only to the extent that they have actually been run and checked.

---

## 2. Step-by-Step AI Interaction & Implementation Log

The following sections connect the AI assistance to my assigned responsibilities and to the corresponding modules in the DART prototype.

### Step 1: Understanding the Proposal and Dividing the Assigned Work

#### 1.1 Objective

The proposal describes a complete reliable UDP transport with three ARQ protocols, a channel emulator, adaptive transmission, integrity checking, and reproducible experiments. My individual ownership is Stop-and-Wait ARQ, the channel emulator, experiment automation/logging, and performance benchmarking. I needed to distinguish my responsibilities from the work assigned to the other team members.

#### 1.2 Prompt Given to AI (summary)

```text
Read the DART project proposal and identify everything assigned to Kartikey.
Turn my responsibilities into an implementation plan. Explain the modules,
what each component should do, and how my work connects to the other
team members' ARQ implementations and the adaptive engine.
```

#### 1.3 AI Response Summary

AI helped extract the ownership and weekly plan from the proposal, and suggested organising the implementation around the sender, receiver, channel emulator, RTO estimator, experiment runner, and test suite.

#### 1.4 Follow-up and Human Responsibility

The proposal was used as the source of truth for the ownership split. The implementation and viva explanation must distinguish my primary responsibilities from Go-Back-N, Selective Repeat, and the adaptive engine, which are shared or assigned elsewhere in the team plan. I need to verify any later changes against the team's current repository and integration decisions.

---

### Step 2: Stop-and-Wait ARQ over UDP

#### 2.1 Architectural Objective

Stop-and-Wait is the ARQ protocol assigned to me. It allows only one unacknowledged data packet to be outstanding at a time. The sender transmits a packet, waits for an acknowledgement, and sends the next packet only after the acknowledgement advances the sender's state. If the packet or its ACK is lost, timeout-based retransmission provides recovery.

#### 2.2 Prompt Given to AI (summary)

```text
Help me understand and implement Stop-and-Wait ARQ for reliable file transfer
using Python UDP sockets. Explain the sender and receiver state, sequence
numbers, ACK handling, retransmission timers, retry limits, duplicate packet
handling, and how to test data loss and ACK loss. Keep the design compatible
with the DART packet format and file-transfer flow.
```

#### 2.3 AI Response Summary

AI explained the Stop-and-Wait state machine and the distinction between data loss and ACK loss. It helped outline the sender's wait/retransmit behaviour and the receiver's expected-sequence check so that a duplicate packet is not written to the output file twice.

#### 2.4 Implementation Areas to Understand (`dart/sender.py` and `dart/receiver.py`)

The current prototype includes the following relevant logic:

- In the sender, the in-flight limit is set to one for non-adaptive Stop-and-Wait mode:

```python
max_inflight = 1 if mode == 'stop-and-wait' and not adaptive else (window if not adaptive else controller.window)
```

- The sender records send times and attempt counts, waits for ACKs, and retransmits packets whose timeout has expired. A retry limit prevents endless retransmission.
- In the receiver, Stop-and-Wait and Go-Back-N share the in-order receive path. The receiver writes a packet only when its sequence number equals the next expected sequence number:

```python
if pkt.seq == tr['expected']:
    tr['file'].write(pkt.payload)
    tr['written'] += len(pkt.payload)
    tr['expected'] += 1
```

- The receiver sends a cumulative acknowledgement containing the next expected sequence number. If a duplicate packet arrives, it does not satisfy the expected-sequence condition, so it is not written again; the receiver still sends the current ACK state.

#### 2.5 Viva Preparation

Topics to explain include why the sender window is one, what `base` and `next_seq` represent, how the receiver's `expected` sequence number works, why ACK loss can cause a duplicate DATA packet, why the duplicate must not be written twice, and how the timeout/retry limit works. I should be able to trace one successful transfer and one lost-ACK scenario using the actual code.

---

### Step 3: Configurable UDP Channel Emulator

#### 3.1 Architectural Objective

The channel emulator provides controlled network impairments so that the reliability mechanisms can be evaluated under conditions other than a perfect local network. The proposal calls for configurable packet loss, duplication, reordering, corruption, delay, and jitter, with a configurable seed for repeatable experiments.

#### 3.2 Prompt Given to AI (summary)

```text
Help design a Python UDP channel emulator/proxy that can apply configurable
packet loss, duplication, corruption, reordering, delay, and jitter. Explain
how to use a seeded random generator so tests can be repeated, and how to
schedule delayed packets without blocking all other packets unnecessarily.
```

#### 3.3 AI Response Summary

AI explained the proxy approach: receive a datagram, apply configured impairment decisions, and forward it to the appropriate destination. It also suggested a seeded pseudo-random generator and a priority queue for packets with different delivery deadlines.

#### 3.4 Implementation Areas to Understand (`dart/emulator.py`)

The current emulator contains:

- `ChannelConfig` for impairment settings and validation.
- `ChannelEmulator` for the UDP proxy behaviour.
- `random.Random(config.seed)` for seeded pseudo-random decisions.
- `_schedule()` for applying impairment decisions and placing packets in the delivery queue.
- `heapq` and `time.monotonic()` for scheduling packets by delivery deadline.
- UDP socket receive/forward operations for moving datagrams between endpoints.

The emulator supports loss, duplication, corruption, reordering, delay, and jitter configuration. The exact impairment probabilities and timing values used in a particular experiment must be read from that run's configuration rather than assumed.

#### 3.5 Viva Preparation

I should be able to explain why a proxy is useful, why both DATA and ACK traffic can be impaired, what a deterministic seed does and does not guarantee, how a priority queue schedules delayed packets, the difference between delay and jitter, and how corruption differs from packet loss. I should also be able to explain that the emulator is a test tool, not a replacement for the transport protocol.

---

### Step 4: Experiment Automation and Performance Logging

#### 4.1 Architectural Objective

Manual testing across many combinations of ARQ mode and network conditions is slow and difficult to reproduce. The experiment runner automates local transfers and collects comparable results. The sender also records event-level information so that retransmissions, ACKs, timeouts, RTT estimates, and window behaviour can be inspected.

#### 4.2 Prompt Given to AI (summary)

```text
Help organise an automated Python experiment runner for the DART UDP file
transfer. It should start the receiver and emulator, run selected ARQ modes
under configured loss/reordering/delay/window/timeout conditions, collect
transfer statistics, and save results to CSV. Explain useful metrics and
how to avoid treating failed transfers as successful benchmark results.
```

#### 4.3 AI Response Summary

AI helped identify the need for a repeatable trial function, structured result rows, CSV output, and a separation between event logs and aggregate benchmark results. It also explained why file size, seed, and network settings should be held constant when comparing configurations.

#### 4.4 Implementation Areas to Understand (`dart/experiments.py` and `dart/sender.py`)

The experiment runner includes functions such as:

- `free_port()` to select an available local port.
- `wait_port()` to wait for a process/service to become ready.
- `trial()` to execute an individual experiment.
- `write_csv()` to save result rows.
- `plot_results()` to generate graphs when plotting support is available.
- `main()` to construct and run the experiment matrix.

The sender can write an event log with fields including `time_s`, `event`, `seq`, `rto_s`, `srtt_s`, `window`, and `retransmissions`. The aggregate benchmark records include transfer and configuration information such as bytes, packet counts, retransmissions, timeouts, ACKs, RTT samples, elapsed time, goodput, integrity, loss probability, reordering probability, window, timeout, and seed.

#### 4.5 Viva Preparation

I should be able to distinguish event logging from benchmarking, explain why the experiment runner uses separate processes, describe the purpose of a repeatable seed, and explain how failed transfers are recorded. I should not claim that every possible combination has been tested merely because the experiment runner supports a range of parameters.

---

### Step 5: Understanding Metrics and Benchmark Interpretation

#### 5.1 Architectural Objective

The proposal evaluates the adaptive strategy against a fixed-parameter reliable UDP baseline. The required P3 experiments include goodput versus loss rate, goodput versus window size, retransmissions versus reordering probability, and sensitivity to RTO tuning.

#### 5.2 Prompt Given to AI (summary)

```text
Explain the DART performance metrics in a way I can defend in a networking
viva. Include goodput, completion time, retransmissions, RTT, timeout
frequency, window behaviour, and file integrity. Explain the four required
P3 experiments and what conclusions can and cannot be drawn from them.
```

#### 5.3 AI Response Summary

AI explained the metrics and the expected qualitative effects of loss, reordering, window size, and timeout configuration. It also highlighted that expected trends are hypotheses to investigate, not measured results.

#### 5.4 Interpretation Responsibilities

- **Goodput:** useful application data delivered per unit time.
- **Completion time:** elapsed time for the transfer process being measured.
- **Retransmissions:** packets sent again because delivery was not confirmed.
- **RTT:** time between a transmission and its corresponding acknowledgement.
- **Timeout frequency:** number of timeout events.
- **Window behaviour:** how many packets may be outstanding under the selected mode/controller.
- **Integrity:** whether the reconstructed file matches the expected SHA-256 digest and byte count.

The current quick benchmark is a smoke test of a reduced experiment matrix. It should not be presented as complete proof that adaptive DART outperforms the fixed baseline. Such a claim requires matched experiments, sufficient repeated trials, and careful comparison of the resulting measurements.

---

### Step 6: Troubleshooting Local Execution on Windows

#### 6.1 Objective

During local execution, the sender command used `your_file.pdf` as an example placeholder. Python reported a file-not-found error because a file with that literal name was not present in the current working directory.

#### 6.2 Prompt Given to AI (summary)

```text
Explain this Python FileNotFoundError when running the DART sender in
PowerShell. Show how to list files in the current directory, use an actual
filename or full path, and run the receiver in a second terminal.
```

#### 6.3 AI Response Summary

AI explained that the example filename must be replaced with a real file path. It provided PowerShell commands to list files, start the receiver, and run the sender with an existing file.

#### 6.4 Follow-up Responsibility

Before running a transfer, I must confirm that the input file exists, that the receiver is running on the expected host and port, and that the received file appears in the output directory. A command copied from an example must be adapted to the actual local file path.

---

## 3. Summary of AI Tool Usage by Category

| Category | AI assistance | My responsibility / follow-up |
|---|---|---|
| Proposal interpretation | Helped extract the assigned tasks and turn them into an implementation plan. | Follow the proposal and coordinate interfaces with the team. |
| Stop-and-Wait concepts | Explained sequence numbers, ACKs, timeout, retransmission, and duplicate handling. | Trace and explain the sender/receiver code and validate behaviour. |
| Channel emulator | Suggested the proxy model, seeded randomness, and impairment test cases. | Verify configuration, run controlled trials, and understand the scheduling logic. |
| Experiment automation | Suggested trial structure, metrics, CSV logging, and plotting workflow. | Run experiments, inspect generated files, and report only verified results. |
| Debugging | Explained a Windows file-path error and how to run the commands correctly. | Use valid local paths and confirm the transfer result. |

AI assistance included planning, explanations, code-oriented suggestions, and troubleshooting. The implementation must be reviewed and understood by the team; this disclosure does not imply that AI suggestions were accepted without review or that all proposed tests have already been completed.

---

## 4. Individual Viva Readiness & Practical Demonstration Plan

To demonstrate understanding of my assigned work, I should be prepared to perform and explain the following:

1. **Stop-and-Wait sender:** Show where the in-flight limit is restricted to one, and explain `base`, `next_seq`, send timestamps, attempts, ACK processing, and timeout handling.
2. **Stop-and-Wait receiver:** Show the `expected` sequence-number check and explain why duplicate packets are not written again.
3. **Loss scenario:** Run a transfer through the emulator with packet loss and explain how timeout and retransmission recover from missing DATA or ACK packets.
4. **Channel emulator:** Show the configuration for loss, duplication, corruption, reordering, delay, jitter, and seed; explain the scheduling queue.
5. **Event log:** Open the sender CSV log and explain the event, sequence number, RTO, SRTT, window, and retransmission columns.
6. **Experiment runner:** Explain how a trial is started, how result rows are collected, and how errors are represented.
7. **Integrity check:** Compare the source and received file hashes and explain why CRC-32 packet checking and SHA-256 file verification serve different purposes.
8. **Benchmark interpretation:** Explain the four required P3 experiments and distinguish expected trends from actual measured results.

Any live demonstration should use the current code and actual output. I should not claim a fault-injection result, benchmark improvement, or test count unless I have run it and can show the evidence.

---

## 5. Formal Declaration

I acknowledge that AI assistance was used during the planning, learning, coding-assistance, and troubleshooting process for the DART project. This record describes the main interactions relevant to my assigned responsibilities and is intended to make that assistance transparent. I remain responsible for reviewing the implementation, following the course's AI-use and library restrictions, validating the results, coordinating integration with the team, and being able to explain and modify my assigned components during the individual viva.

**Student:** Kartikey Singh  
**Roll Number:** 24051713  
**Section:** CSE 34  
**Project:** DART — Dynamic Adaptive Reliable Transport
