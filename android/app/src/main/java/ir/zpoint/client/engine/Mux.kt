package ir.zpoint.client.engine

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * smux-lite multiplexer — port of core/mux.py v2.
 * seq assignment + send enqueue happen atomically (audit target #1 fix);
 * per-stream credit accounting with cumulative half-window grants.
 */
class Mux(
    private val sendFn: (Frame) -> Unit,
    private val crypto: FrameCrypto,
    isServer: Boolean,
    private val clientLabel: String = "c1",
    val window: Int = 512 * 1024
) {
    private val grant = window / 2
    private val seq = AtomicLong(0)
    private val ioLock = Object()                    // seq+send atomicity
    private val nextSid = AtomicInteger(if (isServer) 0 else 1)

    inner class Stream(val sid: Int) {
        val recvQueue = ArrayDeque<ByteArray>()
        @Volatile var open = true
        var txCredits = window.toLong()
        var txUsed = 0L
        var recvBytes = 0L
        // BUGFIX (parity with core #12): ack accounting starts at ZERO.
        // Starting at `grant` deferred the first T_CREDIT to 2*grant
        // (512KB) while senders can only use window - (window mod chunk)
        // — a guaranteed downlink stall after one window.
        var recvGranted = 0L
    }

    val streams = ConcurrentHashMap<Int, Stream>()
    var onOpen: ((Int, String, Int) -> Unit)? = null
    var onData: ((Int, ByteArray) -> Unit)? = null
    var onClose: ((Int) -> Unit)? = null
    /** BUGFIX #22: control-stream handshake frame arrived. */
    var onHello: ((String) -> Unit)? = null
    @Volatile var closed = false

    fun openStream(host: String, port: Int): Int {
        val sid = nextSid.getAndAdd(2)
        streams[sid] = Stream(sid)
        push(sid, Frames.T_OPEN, "$clientLabel@$host:$port".toByteArray())
        return sid
    }

    fun write(sid: Int, data: ByteArray, creditTimeoutMs: Long = 30_000) {
        var off = 0
        while (off < data.size) {
            val end = minOf(off + Frames.MAX_PLAIN, data.size)
            val chunk = data.copyOfRange(off, end)
            waitCredit(sid, chunk.size.toLong(), creditTimeoutMs)
            if (closed) throw java.io.IOException("mux closed")
            push(sid, Frames.T_DATA, chunk)
            off = end
        }
    }

    fun closeStream(sid: Int) {
        val st = streams[sid] ?: return
        if (!st.open) return
        st.open = false
        if (!closed) push(sid, Frames.T_CLOSE, ByteArray(0))
        drop(sid)
    }

    private fun waitCredit(sid: Int, n: Long, timeoutMs: Long) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (true) {
            val st = streams[sid]
            if (st == null || closed) return
            synchronized(st) {
                if (st.txCredits - st.txUsed >= n) return
            }
            if (System.currentTimeMillis() > deadline)
                throw java.io.IOException("credit timeout on stream $sid (${n}B)")
            Thread.sleep(5)
        }
    }

    private fun push(sid: Int, type: String, body: ByteArray) {
        if (closed) return
        val frame: Frame
        synchronized(ioLock) {
            if (closed) return
            val s = seq.incrementAndGet()
            if (type == Frames.T_DATA) streams[sid]?.let { st -> st.txUsed += body.size }
            frame = Frames.pack(s, sid, type, body, crypto)
        }
        sendFn(frame)   // inside ioLock: preserves enqueue order == seq order
    }

    /** Deliver a decrypted inbound frame (called by the ingest thread). */
    fun deliver(node: Frame, rxCrypto: FrameCrypto) {
        val (sid, type, body) = Frames.unpack(node, rxCrypto)
        when (type) {
            Frames.T_OPEN -> {
                val meta = String(body)
                val at = meta.indexOf('@')
                val hp = if (at >= 0) meta.substring(at + 1) else meta
                val host = hp.substringBeforeLast(':')
                val port = hp.substringAfterLast(':').toInt()
                val st = streams.getOrPut(sid) { Stream(sid) }
                st.open = true
                onOpen?.invoke(sid, host, port)
            }
            Frames.T_DATA -> {
                val st = streams[sid]
                if (st != null) {
                    synchronized(st) {
                        st.recvQueue.addLast(body)
                        (st as Object).notifyAll()
                    }
                    grantsFor(sid, body.size.toLong()).forEach { g ->
                        push(sid, Frames.T_CREDIT, g.toString().toByteArray())
                    }
                    onData?.invoke(sid, body)
                }
            }
            Frames.T_CLOSE -> {
                onClose?.invoke(sid)
                drop(sid)
            }
            Frames.T_CREDIT -> {
                val st = streams[sid] ?: return
                synchronized(st) {
                    st.txCredits += String(body).toLongOrNull() ?: 0
                    (st as Object).notifyAll()
                }
            }
            // BUGFIX #22: liveness frame. Deliberately does NOT touch the
            // stream table -- sid 0 is not a real stream, and creating an
            // entry for it would leak a Stream object per handshake.
            Frames.T_HELLO -> onHello?.invoke(String(body))
            Frames.T_PING -> {}
        }
    }

    /** Account inbound bytes on a stream and return the T_CREDIT grants
     *  owed to the peer (cumulative, half-window) — core grantsFor parity.
     *  Also used internally by deliver() to push credits back. */
    fun grantsFor(sid: Int, n: Long): List<Long> {
        val st = streams[sid] ?: return emptyList()
        val grants = mutableListOf<Long>()
        synchronized(st) {
            st.recvBytes += n
            while (st.recvBytes - st.recvGranted >= grant) {
                st.recvGranted += grant
                grants += grant.toLong()
            }
        }
        return grants
    }

    private fun drop(sid: Int) {
        val st = streams.remove(sid) ?: return
        synchronized(st) {
            st.open = false
            (st as Object).notifyAll()
        }
    }

    // ---------- control-frame helpers (v3 parity with core/mux.py) ----------

    /** BUGFIX #22: send the handshake JSON on the control stream.
     *  Routed through push() so it is zlib-compressed, AES-256-GCM sealed
     *  and given a real monotonic transport seq, exactly like every other
     *  frame. That is the whole point: an ack can only come back if the
     *  peer actually holds the session key. */
    fun sendHello(json: String) {
        push(Frames.CTRL_SID, Frames.T_HELLO, json.toByteArray())
    }

    /** BUGFIX #24: send a properly SEALED T_CREDIT grant.
     *
     *  GasClientNode used to hand-build `Frame(0, sid, T_CREDIT, "262144")`
     *  and pass it straight to sendUp(), bypassing Frames.pack() entirely.
     *  Two consequences, both fatal:
     *    1. the body went on the wire as PLAINTEXT, so the Python exit ran it
     *       through unpack_frame -> AES-GCM open -> InvalidTag and dropped
     *       every single grant ("[gas-exit] bad frame"). The exit's tx
     *       credits were therefore never replenished, and any stream larger
     *       than one 512KB window froze permanently -- the classic
     *       "page loads 90% then hangs";
     *    2. transport seq 0 collided with the GCM nonce of the first real
     *       frame, which is a key-reuse hazard, not just a bug.
     *  push() fixes both: real seq, real seal. */
    fun sendCredit(sid: Int, grant: Long) {
        push(sid, Frames.T_CREDIT, grant.toString().toByteArray())
    }

    /** Peer granted transmit credit (applied by GasClientNode ingest). */
    fun applyCredit(sid: Int, nbytes: Int) {
        val st = streams[sid] ?: return
        synchronized(st) {
            st.txCredits += nbytes
            (st as Object).notifyAll()
        }
    }

    /** Peer sent T_CLOSE: drop the stream (wake recv()/waitCredit). */
    fun peerClosed(sid: Int) { drop(sid) }

    /** Defensive: create the recv queue for a stream we didn't open. */
    fun peerOpened(sid: Int) {
        streams.getOrPut(sid) { Stream(sid) }
    }

    /** Pop next chunk; null on timeout/close. Blocks on monitor, not polling. */
    fun recv(sid: Int, timeoutMs: Long): ByteArray? {
        val st = streams[sid] ?: return null
        val deadline = if (timeoutMs <= 0) 0L
                       else System.currentTimeMillis() + timeoutMs
        synchronized(st) {
            while (true) {
                st.recvQueue.removeFirstOrNull()?.let { return it }
                if (!st.open || closed) return null
                if (timeoutMs <= 0) return null
                val left = deadline - System.currentTimeMillis()
                if (left <= 0) return null
                (st as Object).wait(minOf(left, 250))
            }
        }
    }
}
