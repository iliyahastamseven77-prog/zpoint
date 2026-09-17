package ir.zpoint.client.engine

import java.io.ByteArrayOutputStream
import java.util.zip.Deflater
import java.util.zip.Inflater

/**
 * Frame codec mirroring core/mux.py v2.
 * Wire JSON: {"i": seq, "s": stream, "t": type, "b": b64(zlib(ct))}
 * types: o=open, d=data, c=close, k=ping, w=credit
 * AAD = big-endian (stream:u32, type:u8) — must match Python pack("!IB").
 */
object Frames {
    const val T_OPEN = "o"
    const val T_DATA = "d"
    const val T_CLOSE = "c"
    const val T_PING = "k"
    const val T_CREDIT = "w"

    /** BUGFIX #22: liveness handshake frame.
     *  Carried on the control stream (sid 0), which the mux never allocates
     *  to a real stream (client sids are odd from 1, exit sids even from 0
     *  via openStream only), so it can never collide with user data.
     *  Unknown frame types fall through both the Kotlin `when` and the
     *  Python if/elif chains, so an UNPATCHED peer ignores this frame
     *  instead of crashing -- it simply never completes a handshake. */
    const val T_HELLO = "h"
    const val CTRL_SID = 0

    const val MAX_PLAIN = 60 * 1024

    fun aad(stream: Int, type: Char): ByteArray {
        val b = ByteArray(5)
        b[0] = ((stream ushr 24) and 0xff).toByte()
        b[1] = ((stream ushr 16) and 0xff).toByte()
        b[2] = ((stream ushr 8) and 0xff).toByte()
        b[3] = (stream and 0xff).toByte()
        b[4] = type.code.toByte()
        return b
    }

    fun zlibCompress(data: ByteArray): ByteArray {
        if (data.isEmpty()) return ByteArray(0)
        val d = Deflater(6, false)
        d.setInput(data); d.finish()
        val out = ByteArrayOutputStream(data.size / 2 + 32)
        val buf = ByteArray(16384)
        while (!d.finished()) out.write(buf, 0, d.deflate(buf))
        d.end()
        return out.toByteArray()
    }

    fun zlibDecompress(data: ByteArray): ByteArray {
        if (data.isEmpty()) return ByteArray(0)
        val i = Inflater(false)
        i.setInput(data)
        val out = ByteArrayOutputStream(data.size * 4)
        val buf = ByteArray(16384)
        try {
            while (!i.finished()) {
                val n = i.inflate(buf)
                if (n == 0 && i.needsInput()) break
                out.write(buf, 0, n)
            }
        } finally {
            i.end()
        }
        return out.toByteArray()
    }

    fun pack(seq: Long, stream: Int, type: String, body: ByteArray,
             crypto: FrameCrypto): Frame {
        val ct = crypto.seal(seq, zlibCompress(body), aad(stream, type[0]))
        return Frame(seq, stream, type, FrameCrypto.b64encode(ct))
    }

    fun unpack(node: Frame, crypto: FrameCrypto): Triple<Int, String, ByteArray> {
        val ct = FrameCrypto.b64decode(node.b)
        val plain = crypto.open(node.i, ct, aad(node.s, node.t[0]))
        return Triple(node.s, node.t, zlibDecompress(plain))
    }
}

/** Raw wire frame (parsed or to-be-sent). */
data class Frame(val i: Long, val s: Int, val t: String, val b: String,
                 /** envelope epoch this frame arrived in (dedupe generation) */
                 var ep: Long = 0L)
