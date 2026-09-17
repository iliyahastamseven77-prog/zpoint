#!/usr/bin/env bash
# Verify all 5 fixes inside the packed zip.
set -u
cd /tmp && rm -rf zv && mkdir zv && cd zv && unzip -q /root/projects/zpoint/zpoint.zip
echo "1) deliver_down removed: $(grep -c deliver_down core/client.py || true) refs (want 0)"
echo "2) socks reply bytes: $(grep -c 'x05\\x00\\x00\\x01' core/net.py) (want >=1)"
echo "3) exit down root: $(grep -o 'down_root = f"[^"]*"' core/exit.py)"
echo "   client down:     $(grep -o 'down_path = f"[^"]*"' core/client.py)"
echo "4) cumulative credits: $(grep -c _recv_granted core/mux.py) refs (want >=3)"
echo "5) exit mux uses crypto_down: $(grep -c 'Mux(self._send_down, self.crypto_down' core/exit.py)"
