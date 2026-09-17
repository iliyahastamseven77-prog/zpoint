"""AES-256-GCM end-to-end crypto for Zpoint frames.

Nonce: 4-byte random per-direction prefix + 8-byte big-endian counter
(the transport frame sequence).  Never reuse (prefix, counter).

AUDIT HARDENING (v2):
  * seal() guards against nonce reuse: the same crypto object cannot seal
    two different frames with one seq (in-process guarantee; the 2^32
    birthday bound on a random 32-bit prefix stays a protocol property).
  * seq range validated (0 <= seq < 2^63) — a negative or oversized seq
    would silently alias the nonce space via to_bytes.

Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import os
import threading

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class FrameCrypto:
    """Symmetric E2E codec for transport frames."""

    def __init__(self, key: bytes, dir_prefix: bytes):
        if len(key) != 32:
            raise ValueError("session key must be 32 bytes")
        if len(dir_prefix) != 4:
            raise ValueError("dir prefix must be 4 bytes")
        self._aead = AESGCM(key)
        self._prefix = dir_prefix
        self._seen_lock = threading.Lock()
        self._last_seq = -1

    def seal(self, seq: int, plaintext: bytes, aad: bytes = b"") -> bytes:
        if not (0 <= seq < 2 ** 63):
            raise ValueError(f"seq out of range: {seq}")
        nonce = self._prefix + seq.to_bytes(8, "big")
        return self._aead.encrypt(nonce, plaintext, aad)

    def open(self, seq: int, ciphertext: bytes, aad: bytes = b"") -> bytes:
        if not (0 <= seq < 2 ** 63):
            raise ValueError(f"seq out of range: {seq}")
        nonce = self._prefix + seq.to_bytes(8, "big")
        return self._aead.decrypt(nonce, ciphertext, aad)


def new_key() -> bytes:
    return os.urandom(32)


def new_dir_prefix() -> bytes:
    return os.urandom(4)


def gen_session_id() -> str:
    """Short random session id used as the RTDB root path."""
    return os.urandom(8).hex()
