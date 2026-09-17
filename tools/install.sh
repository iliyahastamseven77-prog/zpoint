#!/usr/bin/env bash
# Install zpoint system-wide: symlink menu, compile core, self-test.
# Property of the @ily_bio research channel (@iliyahsatam).
set -euo pipefail

# 1) resolve the real script dir even if the project was unpacked elsewhere
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ZP_ROOT="${ZP_ROOT_OVERRIDE:-$SCRIPT_DIR}"

# 2) exec bits
chmod +x "$ZP_ROOT/bin/zpoint" "$ZP_ROOT/bin/zpointd" "$ZP_ROOT/tools/"*.sh

# 3) the global command — hard symlink (re-created on every run)
mkdir -p /usr/local/bin
ln -sfn "$ZP_ROOT/bin/zpoint" /usr/local/bin/zpoint
hash -r 2>/dev/null || true

# 4) sanity: the command resolves and prints the menu banner
if ! command -v zpoint >/dev/null 2>&1; then
  echo "ERROR: zpoint not on PATH after install" >&2
  exit 1
fi

# 5) quick self-test: crypto + mux roundtrip + credit accounting
#    (dynamic path resolution: ZP_ROOT is resolved from THIS script's own
#    location above — never a hardcoded /root/projects/zpoint; ZP_ROOT
#    is exported so the heredoc python can pick it up from the env)
export ZP_ROOT
python3 - <<'PY'
import os
import sys
sys.path.insert(0, os.environ["ZP_ROOT"])
from core.crypto import FrameCrypto, new_key, new_dir_prefix
from core.mux import pack_frame, unpack_frame
k, up, down = new_key(), new_dir_prefix(), new_dir_prefix()
c = FrameCrypto(k, up)
f = pack_frame(7, 3, "d", b"hello zpoint", c)
seq, sid, t, body = unpack_frame(f, c)
assert (seq, sid, t, body) == (7, 3, "d", b"hello zpoint")
print("self-test OK: crypto+mux roundtrip")
PY

echo "✔ zpoint installed: type  zpoint  in any directory"
echo "  (menu: kits / bench / bandwidth / services)"
