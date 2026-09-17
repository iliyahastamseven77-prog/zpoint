package ir.zpoint.client.engine

import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket

/**
 * SOCKS5 no-auth CONNECT listener — mirrors core/net.py.
 * Reply sent BEFORE bridging (BUGFIX #2), exact 10-byte success frame.
 */
object Socks5 {

    /**
     * Optional sink for swallowed per-session errors.
     *
     * PRIVACY FIX: this used to default to stderr, i.e. logcat. Combined with
     * the "host resolved: <hostname>" trace below, every single domain the
     * user visited was written to the system log of a censorship-circumvention
     * tool -- readable by any process holding READ_LOGS, by adb, and by any
     * bug-report capture. On a seized or compromised phone that log is a
     * complete browsing history.
     *
     * Default is now a no-op. Assign it explicitly (e.g. from a debug build
     * or a developer toggle) when you actually need the trace.
     */
    @Volatile var errLog: (String) -> Unit = {}

    fun serve(listener: ServerSocket, stop: () -> Boolean,
              handler: (Socket, String, Int) -> Unit) {
        Thread({
            while (!stop()) {
                try {
                    val c = listener.accept()
                    Thread({ session(c, handler) }, "zp-socks").apply {
                        isDaemon = true; start()
                    }
                } catch (e: Exception) {
                    if (stop()) return@Thread
                    errLog("accept failed: $e")
                }
            }
        }, "zp-accept").apply { isDaemon = true; start() }
    }

    private fun readExact(input: InputStream, n: Int): ByteArray {
        val buf = ByteArray(n)
        var off = 0
        while (off < n) {
            val r = input.read(buf, off, n - off)
            if (r < 0) throw EOFException("socks peer closed")
            off += r
        }
        return buf
    }

    private fun session(c: Socket, handler: (Socket, String, Int) -> Unit) {
        try {
            // handshake budget: first CONNECT can stall behind engine
            // warm-up (downloader sync across the whole pool)
            c.soTimeout = 90_000
            val input = c.getInputStream()
            val output: OutputStream = c.getOutputStream()
            val hdr = readExact(input, 2)
            if (hdr[0].toInt() != 5) { c.close(); return }
            // CRASH FIX: Kotlin's Byte is SIGNED, so a greeting advertising
            // more than 127 auth methods gave a negative length and
            // readExact() threw NegativeArraySizeException. `and 0xFF` is
            // mandatory on every byte-derived length in this file.
            readExact(input, hdr[1].toInt() and 0xFF)
            output.write(byteArrayOf(0x05, 0x00)); output.flush()
            errLog("greeting ok from ${c.remoteSocketAddress}")
            val req = readExact(input, 4)
            errLog("request: cmd=${req[1]} atyp=${req[3]}")
            if (req[1].toInt() != 1) {              // CONNECT only
                output.write(byteArrayOf(0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0))
                output.flush(); c.close(); return
            }
            val host = when (req[3].toInt()) {
                1 -> InetAddress.getByAddress(readExact(input, 4)).hostAddress!!
                3 -> {
                    // CRASH FIX (same signed-Byte bug, and this one was
                    // reachable from ordinary browsing): a DOMAINNAME length
                    // of 128..255 became negative, so any hostname longer than
                    // 127 characters killed the session thread with
                    // NegativeArraySizeException instead of connecting.
                    // Long CDN and tracking hostnames hit this routinely.
                    val ln = readExact(input, 1)[0].toInt() and 0xFF
                    errLog("host len=$ln")
                    String(readExact(input, ln), Charsets.ISO_8859_1)
                }
                4 -> InetAddress.getByAddress(readExact(input, 16)).hostAddress!!
                else -> { c.close(); return }
            }
            errLog("host resolved: $host")
            // BUGFIX (the handshake hang): the port was read as TWO separate
            // readExact calls (2 bytes + 1 more byte) — the third byte never
            // arrives in the SOCKS wire format, so the handshake stalled
            // forever and the app "connected" without ever moving data.
            val p = readExact(input, 2)
            val port = ((p[0].toInt() and 0xff) shl 8) or
                       (p[1].toInt() and 0xff)
            errLog("port read: $port")
            // BUGFIX #2: reply before handing off — 10-byte success frame
            output.write(byteArrayOf(0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0))
            output.flush()
            errLog("CONNECT reply sent: $host:$port")
            c.soTimeout = 0
            handler(c, host, port)
        } catch (e: Exception) {
            errLog("session ${c.remoteSocketAddress}: " +
                   "${e.javaClass.simpleName}: ${e.message}")
            try { c.close() } catch (_: Exception) {}
        }
    }
}
