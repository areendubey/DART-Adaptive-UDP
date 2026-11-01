"""Shared loopback harness for Go-Back-N tests."""

from __future__ import annotations

import socket
import threading
from typing import Optional

from dart.compat import ReliabilityController, RttEstimator
from dart.constants import DEFAULT_MSS, DEFAULT_TIMEOUT_S, DEFAULT_WINDOW_SIZE
from dart.gbn import GBNStats, GoBackNReceiver, GoBackNSender
from tests.lossy_socket import LossySocket


def make_channel(raw_sock, drop_prob=0.0, corrupt_prob=0.0, seed=1):
    """THE swap point for the channel emulator.

    Right now this wraps the socket in tests/lossy_socket.LossySocket (a small
    private dropper for GBN tests). When the team channel emulator
    (Kartikey Singh) is finalized, return a socket-like object from that
    emulator here instead. Nothing else in the tests needs to change, because
    every test goes through run_gbn_transfer().

    The emulator must offer: sendto(data, addr), recvfrom(bufsize),
    settimeout(value) - the same three calls GoBackNSender/Receiver use.
    """
    return LossySocket(
        raw_sock, drop_prob=drop_prob, corrupt_prob=corrupt_prob,
        seed=seed, drop_outbound=True,
    )


def run_gbn_transfer(
    data: bytes,
    window_size: int = DEFAULT_WINDOW_SIZE,
    timeout: float = DEFAULT_TIMEOUT_S,
    mss: int = DEFAULT_MSS,
    loss_rate: float = 0.0,
    corrupt_rate: float = 0.0,
    seed: int = 1,
    idle_timeout: float = 12.0,
    ack_loss_rate: float = 0.0,
    controller: Optional[ReliabilityController] = None,
    estimator: Optional[RttEstimator] = None,
) -> tuple[bytes, GBNStats, GoBackNReceiver]:
    rx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx_raw.bind(("127.0.0.1", 0))
    tx_raw.bind(("127.0.0.1", 0))
    dest = rx_raw.getsockname()

    tx_sock: socket.socket | LossySocket = tx_raw
    rx_sock: socket.socket | LossySocket = rx_raw
    if loss_rate > 0 or corrupt_rate > 0:
        tx_sock = make_channel(
            tx_raw, drop_prob=loss_rate, corrupt_prob=corrupt_rate, seed=seed
        )
    if ack_loss_rate > 0:
        rx_sock = make_channel(rx_raw, drop_prob=ack_loss_rate, seed=seed + 17)

    receiver = GoBackNReceiver(rx_sock)
    sender = GoBackNSender(
        tx_sock, dest, window_size=window_size, timeout=timeout, mss=mss,
        controller=controller, estimator=estimator,
    )
    box: dict[str, bytes] = {}
    error: list[BaseException] = []

    def run_rx() -> None:
        try:
            box["data"] = receiver.receive_bytes(idle_timeout=idle_timeout)
        except BaseException as exc:  # noqa: BLE001
            error.append(exc)

    thread = threading.Thread(target=run_rx, daemon=True)
    thread.start()
    try:
        stats = sender.send_bytes(data)
        thread.join(timeout=idle_timeout + 2)
        if error:
            raise error[0]
        if "data" not in box:
            raise TimeoutError("receiver did not finish")
        return box["data"], stats, receiver
    finally:
        thread.join(timeout=0.2)
        rx_raw.close()
        tx_raw.close()
