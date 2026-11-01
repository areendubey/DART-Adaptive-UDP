# GBN integration with the shared DART transport (Anushka Roy)

Areen's `dart_transport` package was NOT available when these changes were made,
so each piece below is a **stand-in written from the review notes**. GBN only
imports them through `dart/compat.py`, so switching to the real classes is a
3-line change there. Please check the "assumed interface" lists against the real code.

## 1. Dynamic RTO (Jacobson/Karels + Karn)

| | |
|---|---|
| Stand-in | `dart/rtt.py` -> `RttEstimator` |
| GBN uses | `RttEstimator(initial_rto=...)`, `.rto`, `.srtt`, `.rttvar`, `.update(sample_s)`, `.on_timeout()`, optional `.reset_backoff()` |
| Behaviour | RFC 6298: SRTT/RTTVAR/RTO = SRTT + 4*RTTVAR, clamped to [0.1 s, 2.0 s] |
| `timeout=` argument | now only the **initial** RTO |

How GBN applies Karn's algorithm:
* **No sample from retransmitted packets**: every seq resent on a timeout goes into
  `_retransmitted`; an ACK for such a seq is counted in `stats.rtt_samples_skipped` and not fed to the estimator.
* **Backoff**: each timeout calls `estimator.on_timeout()` (RTO x 2).
* **Backoff reset (design choice)**: when an ACK moves the window forward, `reset_backoff()` is called.
  Reason: GBN resends the whole window on a timeout, so under heavy loss clean samples are rare. Without
  the reset the RTO stayed at the 2 s cap while the measured RTT was 0.4 ms (the 40%-loss test took 49 s,
  now about 10 s). If Areen's estimator has no `reset_backoff()`, GBN skips the call.
* RTT is measured from the **echoed timestamp**: the sender stamps DATA, the receiver copies the stamp into the ACK.
  Repeated (re-)ACKs carry timestamp 0 so they are never used for RTT.

## 2. Unified packet header

| | |
|---|---|
| Stand-in | `dart/packet.py` (+ `dart/constants.py`) -> `Packet`, `PacketType`, `PacketError` |
| Layout | `magic "DART"(4) version(1) flags(1) seq(4) ack(4) timestamp_us(8) payload_len(2) crc32(4)` = 28 bytes + payload |
| GBN uses | `Packet(ptype, seq, ack, payload=b"", timestamp=0)`, `.encode()`, `Packet.decode(raw)` (raises `PacketError`), `.ptype`, `.seq`, `.ack`, `.payload`, `.timestamp`, `.is_data()/.is_ack()/.is_fin()` |

The old 14-byte header with the 16-bit Internet checksum is gone from the wire.
`dart/checksum.py` is no longer used by GBN (left in place, nothing imports it).

**To verify against the real `Packet`:** exact field order/sizes, how flags map to DATA/ACK/FIN,
the timestamp unit (GBN assumes microseconds), and the constructor argument names.

## 3. ReliabilityController hooks

| | |
|---|---|
| Stand-in | `dart/controller.py` -> `ReliabilityController` (base), `StaticController` |
| GBN uses | `.window_size` (read before every send decision), `.on_packet_sent(seq, is_retransmit)`, `.on_acknowledged(ack, newly_acked, rtt_sample_or_None)`, `.on_timeout()` |

```python
# P3 Static mode (default): fixed window
GoBackNSender(sock, dest, window_size=8)

# DART Adaptive mode: Areen's controller decides the window while the transfer runs
GoBackNSender(sock, dest, controller=adaptive_controller)
```

If the window shrinks while packets are in flight, nothing is recalled; GBN just waits
until `outstanding < window`. `stats.min_window_seen / max_window_seen` show how the window moved.

**To verify against the real controller:** hook names and argument order (these are
the names from the review notes: `on_packet_sent`, `on_acknowledged`, `on_timeout`), and how it exposes the window.

## 4. Team channel emulator

Not finished/available yet, so `tests/lossy_socket.py` is **kept**. All tests now get their
lossy socket from one function, `tests/harness.py::make_channel()`. When the shared emulator is ready,
return it from there (it needs `sendto`, `recvfrom`, `settimeout`) and delete `lossy_socket.py`.
Note `DropFirstN` (used by one test) also lives in `lossy_socket.py`.

## Changed files

* `dart/gbn.py`: estimator, controller hooks, single deadline-based timer, timestamp echo
* `dart/packet.py`, `dart/constants.py`: unified header
* `dart/rtt.py`, `dart/controller.py`, `dart/compat.py`: new
* `dart/__init__.py`: exports
* `tests/harness.py`: `make_channel()`, `controller=` / `estimator=` options
* `tests/test_gbn_packet.py`: updated for the new header
* `tests/test_gbn_rtt_controller.py`: new (27 tests)

## One thing noticed but NOT changed

The receiver returns as soon as it accepts FIN. If the final ACK is lost, the sender keeps
retransmitting FIN and no one answers. A short "linger" in the receiver after FIN
(re-ACK repeated FINs for ~1 s) would fix it. Not part of the four requested changes, so left alone.
