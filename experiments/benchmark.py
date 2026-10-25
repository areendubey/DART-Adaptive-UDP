"""
Automated Benchmarking and Experiment Suite for DART Protocol Evaluation.
Catalogue Foundation: P3 Reliable Data Transfer over UDP + Adaptive Transmission Strategy.

Includes:
  - find_free_port helper
  - Automated test runners for:
      1. Goodput vs Loss Rate (Stop-and-Wait vs Go-Back-N vs Selective Repeat)
      2. Goodput vs Window Size (Go-Back-N vs Selective Repeat)
      3. Retransmissions vs Reordering Probability
      4. Sensitivity to RTO Tuning
      5. Static P3 vs DART Adaptive Comparison
"""

import socket
import time
import os
import tempfile
import hashlib
import json
from typing import Dict, Any, List, Optional


def find_free_port() -> int:
    """Finds an available UDP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]
