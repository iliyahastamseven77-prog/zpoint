"""Config load/save for .zpoint profiles.

Profile = JSON with: db_url, auth(optional), session, key(b64),
up_prefix(b64), down_prefix(b64), client_id, kind.

Property of the @ily_bio research channel (@iliyahsatam).
"""
from __future__ import annotations

import base64
import json
import os

MAGIC = "zpoint-config-v1"
ATTR = "Property of the @ily_bio research channel (@iliyahsatam)."


def save(path: str, cfg: dict) -> None:
    cfg = dict(cfg)
    cfg["_magic"] = MAGIC
    cfg["_owner"] = ATTR
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
    os.chmod(path, 0o600)


def load(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        cfg = json.load(f)
    if cfg.get("_magic") != MAGIC:
        raise ValueError(f"{path}: not a zpoint config")
    return cfg


def new_kit(db_url: str, auth: str, kind: str,
            client_id: str = "c1") -> dict:
    """Generate a fresh server-side kit; return both server+client dicts."""
    from .crypto import new_key, new_dir_prefix, gen_session_id
    session = gen_session_id()
    key = new_key()
    up = new_dir_prefix()    # client->exit nonce prefix
    down = new_dir_prefix()  # exit->client nonce prefix
    b64 = lambda b: base64.urlsafe_b64encode(b).decode()
    server = {"kind": "server", "db_url": db_url, "auth": auth,
              "session": session, "key": b64(key),
              "up_prefix": b64(up), "down_prefix": b64(down),
              "client_id": "*"}
    client = {"kind": "client", "db_url": db_url, "auth": auth,
              "session": session, "key": b64(key),
              "up_prefix": b64(up), "down_prefix": b64(down),
              "client_id": client_id}
    return {"server": server, "client": client}
