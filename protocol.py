"""
protocol.py  -  packet format shared by ALL ARQ modes (Stop-and-Wait, GBN, SR).

OWNER: Areen (header design).  This is a working STUB so Selective Repeat can be
built and tested now.  If Areen's final header differs, only THIS file changes -
the rest of the code only uses:  Packet(ptype, seq, ack, payload), .encode(), Packet.decode().

Wire layout (network byte order, 15-byte header):
    | type 1B | seq 4B | ack 4B | length 2B | checksum 4B | payload ... |

Sequence numbers count PACKETS (0, 1, 2, ...), not bytes, and never wrap
(a 32-bit counter is far more than any test file needs).
"""
import struct
import zlib
from dataclasses import dataclass
from typing import Optional

DATA, ACK, FIN = 1, 2, 3          # packet types (FIN = "end of file", sent reliably like data)

_BODY = struct.Struct("!BIIH")    # type, seq, ack, length  (11 bytes)
_CKSUM = struct.Struct("!I")      # checksum                 (4 bytes)
HEADER_SIZE = _BODY.size + _CKSUM.size


def _checksum(body: bytes, payload: bytes) -> int:
    # CRC32 covers header fields + payload. Isolated here so it is easy to swap
    # for the Internet checksum if the course requires a hand-written one.
    return zlib.crc32(body + payload)


@dataclass
class Packet:
    ptype: int
    seq: int = 0            # DATA/FIN: this packet's number
    ack: int = 0            # ACK: the packet number being acknowledged (selective ACK)
    payload: bytes = b""

    def encode(self) -> bytes:
        body = _BODY.pack(self.ptype, self.seq, self.ack, len(self.payload))
        return body + _CKSUM.pack(_checksum(body, self.payload)) + self.payload

    @staticmethod
    def decode(raw: bytes) -> Optional["Packet"]:
        """Return a Packet, or None if the bytes are truncated or corrupted."""
        if len(raw) < HEADER_SIZE:
            return None
        ptype, seq, ack, length = _BODY.unpack_from(raw, 0)
        (cksum,) = _CKSUM.unpack_from(raw, _BODY.size)
        payload = raw[HEADER_SIZE:]
        if len(payload) != length or _checksum(raw[:_BODY.size], payload) != cksum:
            return None
        return Packet(ptype, seq, ack, payload)
