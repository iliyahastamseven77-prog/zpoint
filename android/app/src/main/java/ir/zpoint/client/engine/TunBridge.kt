package ir.zpoint.client.engine

import android.os.ParcelFileDescriptor
import java.io.FileInputStream
import java.io.FileOutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.ByteBuffer

/**
 * TUN packet bridge — the VPN-mode data path.
 *
 * Reads IP packets from the TUN fd, extracts the TCP flow (src ip:port ->
 * dst ip:port), opens a mux stream for the connection (via the SOCKS5
 * listener on loopback — full SOCKS handshake, zero engine changes) and
 * pumps raw TCP payload bytes both ways. TCP state (SYN/ACK, window
 * scaling, retransmits) stays inside the phone's kernel: only the PAYLOAD
 * stream is relayed, so the phone's TCP stack does all the hard work.
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 */
object TunBridge {

    private const val TCP_PROTO = 6

    /** One relayed TCP flow. */
    private class Flow(
        val sock: Socket,
        @Volatile var toTun: Thread? = null,
    )

    /** Pump TUN -> flows and flows -> TUN until [stopFlag] is set. */
    fun run(tunFd: Int, vpn: android.net.VpnService, socksPort: Int,
            mtu: Int, stopFlag: () -> Boolean,
            log: (String) -> Unit = {}) {
        val streams = ParcelFileDescriptor.adoptFd(tunFd)
        val pfdIn = java.io.FileInputStream(streams.fileDescriptor)
        val pfdOut = java.io.FileOutputStream(streams.fileDescriptor)
        val flows = java.util.concurrent.ConcurrentHashMap<String, Flow>()
        val buf = ByteArray(mtu + 32)

        fun closeFlow(key: String, f: Flow) {
            flows.remove(key)
            try { f.sock.close() } catch (_: Exception) {}
        }

        // ---- flows -> TUN (one reader thread per flow; cheap, daemon) ----
        fun pumpToTun(key: String, f: Flow, sink: java.io.FileOutputStream) {
            try {
                val input = f.sock.getInputStream()
                val chunk = ByteArray(mtu)
                while (!stopFlag()) {
                    val n = input.read(chunk)
                    if (n < 0) break
                    // TCP payload over an ESTABLISHED flow -> IP packet
                    val pkt = buildTcpPacket(key, chunk, n)
                        ?: continue   // flow not initialized yet
                    sink.write(pkt)
                }
            } catch (_: Exception) {
            } finally {
                flows.remove(key)
                try { f.sock.close() } catch (_: Exception) {}
            }
        }

        // ---- TUN -> flows ----
        val tin = pfdIn
        val tout = pfdOut
        while (!stopFlag()) {
            val n = try { tin.read(buf) } catch (_: Exception) { break }
            if (n <= 0) break
            val pkt = buf.copyOf(n)
            val ver = (pkt[0].toInt() shr 4) and 0xF
            if (ver != 4) continue                       // IPv4 only (v1)
            val ihl = (pkt[0].toInt() and 0xF) * 4
            if (pkt.size < ihl) continue
            val proto = pkt[9].toInt() and 0xFF
            if (proto != TCP_PROTO) continue             // v1: TCP only
            val srcIp = "${pkt[12].toPositive()}.${pkt[13].toPositive()}." +
                        "${pkt[14].toPositive()}.${pkt[15].toPositive()}"
            val dstIp = "${pkt[16].toPositive()}.${pkt[17].toPositive()}." +
                        "${pkt[18].toPositive()}.${pkt[19].toPositive()}"
            val tcp = pkt.copyOfRange(ihl, pkt.size)
            if (tcp.size < 20) continue
            val srcPort = ((tcp[0].toInt() and 0xFF) shl 8) or (tcp[1].toInt() and 0xFF)
            val dstPort = ((tcp[2].toInt() and 0xFF) shl 8) or (tcp[3].toInt() and 0xFF)
            val flags = tcp[13].toInt() and 0xFF
            val dataOff = ((tcp[12].toInt() shr 4) and 0xF) * 4
            val payload = if (tcp.size > dataOff) tcp.copyOfRange(dataOff, tcp.size)
                          else ByteArray(0)
            val key = "$srcIp:$srcPort->$dstIp:$dstPort"

            val f = flows[key]
            when {
                // SYN -> open a new loopback SOCKS5 connection for this flow
                flags and 0x02 != 0 -> {
                    if (f != null) closeFlow(key, f)
                    val s = Socket()
                    try {
                        vpn.protect(s)
                        s.tcpNoDelay = true
                        s.connect(InetSocketAddress(
                            InetAddress.getByName("127.0.0.1"), socksPort))
                    } catch (e: Exception) {
                        log("[tun] connect socks failed: ${e.message}")
                        continue
                    }
                    if (!socksHandshake(s, dstIp, dstPort)) {
                        try { s.close() } catch (_: Exception) {}
                        log("[tun] socks handshake failed for $key")
                        continue
                    }
                    val nf = Flow(s)
                    flows[key] = nf
                    nf.toTun = Thread({
                        pumpToTun(key, nf, tout)
                    }, "zp-tun-rx").apply { isDaemon = true; start() }
                    if (payload.isNotEmpty()) writeAll(s, payload)
                }
                // established flow -> forward payload
                f != null && payload.isNotEmpty() -> {
                    if (!writeAll(f.sock, payload)) closeFlow(key, f)
                }
                // FIN/RST with no flow -> nothing to do
                flags and (0x01 or 0x04) != 0 && f != null -> closeFlow(key, f)
            }
        }
        flows.values.forEach { try { it.sock.close() } catch (_: Exception) {} }
        flows.clear()
        try { pfdIn.close() } catch (_: Exception) {}
        try { pfdOut.flush() } catch (_: Exception) {}
        try { streams.close() } catch (_: Exception) {}
    }

    private fun writeAll(s: Socket, data: ByteArray): Boolean = try {
        s.getOutputStream().apply { write(data); flush() }
        true
    } catch (_: Exception) { false }

    /** Minimal SOCKS5 CONNECT handshake against our own loopback listener. */
    private fun socksHandshake(s: Socket, host: String, port: Int): Boolean {
        val out = s.getOutputStream()
        val inp = s.getInputStream()
        out.write(byteArrayOf(0x05, 0x01, 0x00)); out.flush()
        if (inp.read() != 5 || inp.read() != 0) return false
        val hh = host.toByteArray(Charsets.ISO_8859_1)
        val req = ByteArray(7 + hh.size)
        req[0] = 5; req[1] = 1; req[2] = 0; req[3] = 3
        req[4] = hh.size.toByte()
        System.arraycopy(hh, 0, req, 5, hh.size)
        req[5 + hh.size] = ((port ushr 8) and 0xFF).toByte()
        req[6 + hh.size] = (port and 0xFF).toByte()
        out.write(req); out.flush()
        // reply: ver(1) rep(1) rsv(1) atyp(1) [addr] port(2)
        val hdr = ByteArray(4)
        if (inp.read(hdr) != 4) return false
        if (hdr[1].toInt() != 0) return false           // rep != succeeded
        val skip = when (hdr[3].toInt()) {
            1 -> 4; 4 -> 16; 3 -> inp.read().coerceAtLeast(0)
            else -> return false
        }
        var left = skip + 2
        val sink = ByteArray(256)
        while (left > 0) {
            val r = inp.read(sink, 0, minOf(left, sink.size))
            if (r < 0) return false
            left -= r
        }
        return true
    }

    /** Build a minimal IPv4+TCP packet carrying [n] payload bytes for the
     *  flow identified by [key]. Only used for flow->TUN (downlink). */
    private fun buildTcpPacket(key: String, payload: ByteArray,
                               n: Int): ByteArray? {
        val parts = key.split("->")
        if (parts.size != 2) return null
        val (src, dst) = parts
        val srcIp = ipBytes(src.substringBefore(':')) ?: return null
        val dstIp = ipBytes(dst.substringBefore(':')) ?: return null
        val srcPort = src.substringAfter(':').toIntOrNull() ?: return null
        val dstPort = dst.substringAfter(':').toIntOrNull() ?: return null
        val tcpLen = 20 + n
        val total = 20 + tcpLen
        val p = ByteArray(total)
        // IPv4 header
        p[0] = 0x45                          // v4, ihl=5
        p[1] = 0                             // DSCP
        p[2] = ((total ushr 8) and 0xFF).toByte()
        p[3] = (total and 0xFF).toByte()
        p[8] = 64                            // TTL
        p[9] = TCP_PROTO.toByte()
        System.arraycopy(srcIp, 0, p, 12, 4)
        System.arraycopy(dstIp, 0, p, 16, 4)
        // TCP header (ACK|PSH, seq/ack values are irrelevant to the kernel's
        // payload-accept path as long as they are in-window; we rely on the
        // fact that the kernel assigned us the seq space at SYN time.)
        p[20] = ((srcPort ushr 8) and 0xFF).toByte()
        p[21] = (srcPort and 0xFF).toByte()
        p[22] = ((dstPort ushr 8) and 0xFF).toByte()
        p[23] = (dstPort and 0xFF).toByte()
        p[32] = 0x50                         // data offset 5 (20B)
        p[33] = 0x18                         // PSH|ACK
        p[36] = ((tcpLen ushr 8) and 0xFF).toByte()   // window (generous)
        p[37] = (tcpLen and 0xFF).toByte()
        System.arraycopy(payload, 0, p, 40, n)
        // checksums: the TUN reader in the kernel accepts zero TCP checksum
        // on loopback TUN (no offload requirement); IPv4 header checksum
        // must be correct though.
        val ipCsum = csum(p, 0, 20)
        p[10] = ((ipCsum ushr 8) and 0xFF).toByte()
        p[11] = (ipCsum and 0xFF).toByte()
        return p
    }

    private fun ipBytes(ip: String): ByteArray? {
        val seg = ip.split(".")
        if (seg.size != 4) return null
        val out = ByteArray(4)
        for (i in 0 until 4) {
            val v = seg[i].toIntOrNull() ?: return null
            if (v < 0 || v > 255) return null
            out[i] = v.toByte()
        }
        return out
    }

    /** RFC1071 checksum over [data] (with the checksum field zeroed). */
    private fun csum(data: ByteArray, off: Int, len: Int): Int {
        var sum = 0L
        var i = off
        while (i < off + len - 1) {
            sum += ((data[i].toPositive() shl 8) or data[i + 1].toPositive())
            i += 2
        }
        if (len % 2 == 1) sum += data[off + len - 1].toPositive() shl 8
        while (sum shr 16 != 0L) sum = (sum and 0xFFFF) + (sum shr 16)
        return (sum.inv() and 0xFFFF).toInt()
    }

    private fun Byte.toPositive(): Int = toInt() and 0xFF
}
