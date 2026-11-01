"""Shared protocol constants used by Go-Back-N."""

PROTOCOL_VERSION = 1

# Magic bytes at the start of every DART packet (unified header).
MAGIC = b"DART"

# Default Go-Back-N parameters
DEFAULT_WINDOW_SIZE = 8
# DEFAULT_TIMEOUT_S is now only the INITIAL RTO (used before the first RTT
# sample arrives). After that the RttEstimator decides the timeout.
DEFAULT_TIMEOUT_S = 0.35
DEFAULT_MSS = 1024
MAX_PAYLOAD = 1400
SEQ_MODULUS = 2**32
SOCKET_BUFFER = 65536

# RTO limits for the Jacobson/Karels estimator (seconds)
RTO_MIN_S = 0.1
RTO_MAX_S = 2.0

# Unified DART header (network byte order), 28 bytes:
#   magic:4s | version:u8 | flags:u8 | seq:u32 | ack:u32 |
#   timestamp_us:u64 | payload_len:u16 | crc32:u32
HEADER_FORMAT = "!4sBBIIQHI"
HEADER_SIZE = 28

# Flag bits
FLAG_DATA = 0x01
FLAG_ACK = 0x02
FLAG_FIN = 0x04
