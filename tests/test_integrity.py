import hashlib
from pathlib import Path
import tempfile
import unittest

from dart_transport.integrity import sha256_file, verify_file_integrity


class IntegrityTests(unittest.TestCase):
    def test_matching_files_pass_and_report_their_sha256(self) -> None:
        content = b"DART reliable transfer\n" * 100
        expected_hash = hashlib.sha256(content).hexdigest()
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "source.bin"
            received = Path(temporary_directory) / "received.bin"
            source.write_bytes(content)
            received.write_bytes(content)

            result = verify_file_integrity(source, received)

        self.assertTrue(result.passed)
        self.assertEqual(result.source_sha256, expected_hash)
        self.assertIn("Integrity      : PASS", result.display())

    def test_different_files_fail_verification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "source.bin"
            received = Path(temporary_directory) / "received.bin"
            source.write_bytes(b"original")
            received.write_bytes(b"corrupted")

            result = verify_file_integrity(source, received)

        self.assertFalse(result.passed)
        self.assertIn("Integrity      : FAIL", result.display())

    def test_hash_uses_incremental_reads_and_validates_block_size(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "payload.bin"
            path.write_bytes(b"123456789")

            self.assertEqual(sha256_file(path, block_size=2), hashlib.sha256(b"123456789").hexdigest())
            with self.assertRaises(ValueError):
                sha256_file(path, block_size=0)
