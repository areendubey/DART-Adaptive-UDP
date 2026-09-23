"""
cli.py  -  send a file with Selective Repeat over real UDP (two terminals / two laptops).

  Terminal 1:  python cli.py recv received.bin --port 9000
  Terminal 2:  python cli.py send myfile.bin --host 127.0.0.1 --port 9000 --window 16 --rto 0.1

Both sides print SHA-256 so you can compare the hashes for the integrity demo.
"""
import argparse
import hashlib
import socket

from config import Config
from rto import FixedRTO, JacobsonKarelsRTO
from selective_repeat import SRReceiver, SRSender


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 16), b""):
            h.update(block)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["send", "recv"])
    ap.add_argument("path")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--window", type=int, default=8)
    ap.add_argument("--rto", type=float, default=0.2)
    ap.add_argument("--adaptive-rto", action="store_true", help="use Jacobson/Karels instead of a fixed RTO")
    a = ap.parse_args()

    cfg = Config(window_size=a.window, initial_rto=a.rto)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    if a.mode == "recv":
        sock.bind(("0.0.0.0", a.port))
        print(f"listening on :{a.port} ...")
        stats = SRReceiver(sock, cfg).receive_file(a.path)
    else:
        rto = JacobsonKarelsRTO(a.rto, cfg.min_rto, cfg.max_rto) if a.adaptive_rto else FixedRTO(a.rto)
        stats = SRSender(sock, (a.host, a.port), cfg, rto).send_file(a.path)

    print(stats.to_row())
    print("sha256:", sha256(a.path))


if __name__ == "__main__":
    main()
