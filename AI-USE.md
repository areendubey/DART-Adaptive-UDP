# AI-USE.md

**Project:** DART-Adaptive-UDP
**Author:** Anushka Roy (24051912)
**My part:** Go-Back-N (`dart/gbn.py`), ACK and retransmission handling, and the reliability tests.

> Before submitting: replace each prompt below with the exact words you used, and remove any row that did not happen.

---

## How I used AI

I used an AI assistant (Claude) to help with my Go-Back-N code and tests. I ran all the tests after each change and read every piece of code before keeping it.

---

## Prompts I gave and why

| # | Prompt | Why I asked |
|---|--------|-------------|
| 1 | "Change my GBN sender to use a dynamic timeout (Jacobson/Karels) and follow Karn's algorithm." | The review notes said a fixed timeout was not adaptive. |
| 2 | "Change my packet header to the shared DART header (magic, flags, timestamp, CRC32)." | The team agreed on one common header. |
| 3 | "Add controller hooks (`on_packet_sent`, `on_acknowledged`, `on_timeout`, `window_size`) so Areen's adaptive controller can set my window." | GBN must work in both Static and Adaptive mode. |
| 4 | "Areen's `dart_transport` is not ready. How do I write GBN so I can switch to it later?" | I could not wait for the shared code. |
| 5 | "Make the tests get the lossy socket from one function." | The team channel emulator was not finished. |
| 6 | "Write tests for the timeout estimator and controller hooks." | I needed tests for the new behaviour. |
| 7 | "Why is my 40% loss test so slow?" | It took about 49 seconds. |
| 8 | "Explain Karn's algorithm and RFC 6298 in simple steps." | I wanted to understand them for the viva. |

---

## What I wrote and what AI wrote

**I wrote:** the original GBN sender and receiver, and the original tests.

**AI helped write:** `dart/rtt.py`, `dart/controller.py`, `dart/compat.py`, the new header code, the timeout and controller changes in `gbn.py`, and the new tests.

I can explain all of it.

---

## Known problems

- `Packet`, `RttEstimator` and `ReliabilityController` in my repo are stand-ins. They must be checked against Areen's real code.
- If the final ACK for FIN is lost, the sender keeps resending FIN. I did not fix this.
- Calling `reset_backoff()` when the window moves forward is my own design choice, not part of RFC 6298.

---

## Things I should be ready to explain in the viva

1. How Go-Back-N works (window, base, one timer, resend from base).
2. What a cumulative ACK is.
3. How the timeout is calculated (SRTT, RTTVAR, RTO).
4. What Karn's algorithm does.
5. What the controller hooks do.
6. Why I call `reset_backoff()`.
