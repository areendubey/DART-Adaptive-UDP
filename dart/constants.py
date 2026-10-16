"""Shared protocol constants used by Go-Back-N.

Packet layout is the DART UDP header GBN needs. Other ARQ modes can reuse it.
"""

PROTOCOL_VERSION = 1

# Default Go-Back-N parameters
DEFAULT_WINDOW_SIZE = 8
DEFAULT_TIMEOUT_S = 0.35
DEFAULT_MSS = 1024
MAX_PAYLOAD = 1400
SEQ_MODULUS = 2**32
SOCKET_BUFFER = 65536

HEADER_FORMAT = "!BBIIHH"  # version, ptype, seq, ack, payload_len, checksum
HEADER_SIZE = 14
