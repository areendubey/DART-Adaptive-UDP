# DART packet format

Every UDP datagram starts with a fixed 34-byte header in network byte order (big-endian), followed by at most 1,200 payload bytes. This is the single packet codec used by every P3 ARQ implementation; DART adds flags and telemetry without a second wire protocol.

| Offset | Size | Field | Validation |
| --- | ---: | --- | --- |
| 0 | 4 | magic | ASCII `DART` |
| 4 | 1 | version | currently `1` |
| 5 | 1 | packet type | DATA, ACK, FIN, or FIN_ACK |
| 6 | 2 | flags | retransmission and selective-ACK flags |
| 8 | 4 | session ID | unsigned transfer identifier |
| 12 | 4 | sequence number | unsigned packet sequence |
| 16 | 4 | acknowledgement | unsigned cumulative/selective ACK value |
| 20 | 8 | timestamp | monotonic microseconds at transmit time |
| 28 | 2 | payload length | 0–1,200 bytes |
| 30 | 4 | checksum | CRC-32 of header with this field zeroed, then payload |

Malformed, version-mismatched, length-mismatched, over-sized, or checksum-failed datagrams raise `PacketDecodeError` and must be discarded before they reach ARQ state. The CRC is an accidental-corruption check, while file-level SHA-256 remains the transfer integrity check.
