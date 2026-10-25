"""File-level SHA-256 verification for reliable UDP transfers."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path

_HASH_BLOCK_SIZE = 64 * 1024


@dataclass(frozen=True, slots=True)
class IntegrityResult:
    """The final end-to-end integrity evidence for one file transfer."""

    source_sha256: str
    received_sha256: str

    @property
    def passed(self) -> bool:
        return self.source_sha256 == self.received_sha256

    def display(self) -> str:
        """Return stable CLI-ready evidence without printing from library code."""
        status = "PASS" if self.passed else "FAIL"
        return (
            f"Source SHA-256 : {self.source_sha256}\n"
            f"Received SHA-256: {self.received_sha256}\n"
            f"Integrity      : {status}"
        )


def sha256_file(path: str | Path, *, block_size: int = _HASH_BLOCK_SIZE) -> str:
    """Hash a file incrementally so verification does not load the full transfer into memory."""
    if block_size <= 0:
        raise ValueError("block size must be positive")
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(block_size), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_file_integrity(source_path: str | Path, received_path: str | Path) -> IntegrityResult:
    """Compare source and received file hashes after transfer completion."""
    return IntegrityResult(
        source_sha256=sha256_file(source_path),
        received_sha256=sha256_file(received_path),
    )
