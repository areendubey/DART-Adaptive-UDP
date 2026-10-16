"""Shared loopback harness for Go-Back-N tests."""

from __future__ import annotations

import socket
import threading
from dart.constants import DEFAULT_MSS, DEFAULT_TIMEOUT_S, DEFAULT_WINDOW_SIZE
from dart.gbn import GBNStats, GoBackNReceiver, GoBackNSender
from tests.lossy_socket import LossySocket


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
) -> tuple[bytes, GBNStats, GoBackNReceiver]:
    rx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tx_raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx_raw.bind(("127.0.0.1", 0))
    tx_raw.bind(("127.0.0.1", 0))
    dest = rx_raw.getsockname()

    tx_sock: socket.socket | LossySocket = tx_raw
    rx_sock: socket.socket | LossySocket = rx_raw
    if loss_rate > 0 or corrupt_rate > 0:
        tx_sock = LossySocket(
            tx_raw,
            drop_prob=loss_rate,
            corrupt_prob=corrupt_rate,
            seed=seed,
            drop_outbound=True,
        )
    if ack_loss_rate > 0:
        rx_sock = LossySocket(
            rx_raw,
            drop_prob=ack_loss_rate,
            seed=seed + 17,
            drop_outbound=True,
        )

    receiver = GoBackNReceiver(rx_sock)
    sender = GoBackNSender(
        tx_sock, dest, window_size=window_size, timeout=timeout, mss=mss
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
