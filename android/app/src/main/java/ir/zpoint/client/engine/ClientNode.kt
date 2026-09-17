package ir.zpoint.client.engine

import org.json.JSONObject
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Zpoint client node — port of core/client.py v2.
 * SOCKS5 :1086 -> mux over RTDB (uplink PUT / downlink SSE + gap-fill).
 * Audit hardening carried over: single ingest thread, lane-ordered writes,
 * control lane for o/c/w, idempotent teardown, single socket owner.
 */
class ClientNode(
    dbUrl: String,
    auth: String,
    private val sessionId: String,
    key: ByteArray,
    upPrefix: ByteArray,
    downPrefix: ByteArray,
    private val clientLabel: String = "c1",
    private val listenPort: Int = 1086,
    private val log: (String) -> Unit = {}
) {
    private val db = Rtdb(dbUrl, auth)
    private val upPath = "z/$sessionId/u"
    private val downPath = "z/$sessionId/d/$clientLabel"
    private val cryptoUp = FrameCrypto(key, upPrefix)
    private val cryptoDown = FrameCrypto(key, downPrefix)
    // WriteQueue hands us (path, json-string); Rtdb.put wants a JSONObject
    private val wq = WriteQueue(putFn = { p, v ->
        try { db.put(p, JSONObject(v)) } catch (_: Exception) { false }
    }, log = log)
    private val ingest = FrameIngest(log)
    private val mux = Mux({ f -> sendUp(f) }, cryptoUp, isServer = false,
                          clientLabel = clientLabel)

    private val sockets = ConcurrentHashMap<Int, Socket>()
    @Volatile private var stopFlag = false
    val stats = ConcurrentHashMap<String, AtomicLong>().apply {
        listOf("frames_in", "frames_out", "bytes_in", "bytes_out", "streams")
            .forEach { put(it, AtomicLong(0)) }
    }

    private fun frameJson(f: Frame): String =
        JSONObject().put("i", f.i).put("s", f.s).put("t", f.t).put("b", f.b).toString()

    private fun sendUp(f: Frame) {
        val lane = if (f.t != Frames.T_DATA) WriteQueue.CONTROL_LANE else "u:${f.s}"
        wq.submit("$upPath/${f.i}", frameJson(f), lane)
        stats["frames_out"]!!.incrementAndGet()
    }

    fun start() {
        val srv = java.net.ServerSocket(listenPort, 64,
                                        java.net.InetAddress.getByName("127.0.0.1"))
        Socks5.serve(srv, { stopFlag }, { sock, host, port -> handleSocks(sock, host, port) })
        Thread({
            db.stream(downPath,
                onEvent = { _, path, data -> onEvent(path, data) },
                onLost = { r -> log("[client] SSE lost: $r — reconnecting") },
                onReady = { gapFill() },
                stop = { stopFlag })
        }, "zp-cli-sse").apply { isDaemon = true; start() }
        log("[client] SOCKS5 on 127.0.0.1:$listenPort session=$sessionId")
    }

    fun stopAll() {
        stopFlag = true
        mux.closed = true
        wq.flush(5000)
        wq.stopAll()
        sockets.values.forEach {
            try { it.shutdownInput() } catch (_: Exception) {}
            try { it.shutdownOutput() } catch (_: Exception) {}
            try { it.close() } catch (_: Exception) {}
        }
        sockets.clear()
    }

    // ---------- inbound (exit -> client) ----------
    private fun onEvent(path: String, data: JSONObject?) {
        if (data == null) return
        // SSE synthetic snapshot: data is a map seq->frame
        val keys = data.names()
        if (keys != null && keys.length() > 0) {
            val first = data.optJSONObject(keys.getString(0))
            if (first != null && first.has("b")) {
                val frames = mutableListOf<Pair<Long, Frame>>()
                for (k in data.keys()) {
                    val v = data.optJSONObject(k) ?: continue
                    if (!v.has("b")) continue
                    val seq = k.removePrefix("/").toLongOrNull()
                        ?: v.optLong("i", -1L).takeIf { it >= 0 }
                        ?: continue
                    frames += seq to Frame(seq, v.optInt("s"), v.optString("t"),
                                           v.optString("b"))
                }
                frames.sortBy { it.first }
                frames.forEach { ingest.accept(it.second, ::consume) }
                return
            }
        }
        if (!data.has("b")) return
        val seq = lastSegment(path).toLongOrNull() ?: data.optLong("i", -1L)
        if (seq < 0) return
        ingest.accept(Frame(seq, data.optInt("s"), data.optString("t"),
                            data.optString("b")), ::consume)
    }

    /** Deterministic gap-fill: one-shot GET of the whole downlink subtree. */
    private fun gapFill() {
        try {
            val snap = db.get(downPath) ?: return
            val frames = mutableListOf<Pair<Long, Frame>>()
            for (k in snap.keys()) {
                val v = snap.optJSONObject(k) ?: continue
                if (!v.has("b")) continue
                val seq = k.removePrefix("/").toLongOrNull()
                    ?: v.optLong("i", -1L).takeIf { it >= 0 }
                    ?: continue
                frames += seq to Frame(seq, v.optInt("s"), v.optString("t"),
                                       v.optString("b"))
            }
            if (frames.isNotEmpty())
                log("[client] gap-fill: ${frames.size} frame(s) recovered")
            frames.sortBy { it.first }
            frames.forEach { ingest.accept(it.second, ::consume) }
        } catch (e: Exception) {
            log("[client] gap-fill scan failed: ${e.message}")
        }
    }

    private fun consume(node: Frame) {
        stats["frames_in"]!!.incrementAndGet()
        try {
            val (sid, type, body) = Frames.unpack(node, cryptoDown)
            if (type == Frames.T_DATA) {
                stats["bytes_in"]!!.addAndGet(body.size.toLong())
                muxDeliver(sid, body)
            }
            db.delete("$downPath/${node.i}")
        } catch (e: Exception) {
            log("[client] bad frame: ${e.message}")
        }
    }

    // data frames from the exit ride the same mux recv queues
    private fun muxDeliver(sid: Int, body: ByteArray) {
        val st = mux.streams[sid]
        if (st != null) synchronized(st) {
            st.recvQueue.addLast(body)
            (st as Object).notifyAll()
        }
    }

    // ---------- SOCKS handler ----------
    private fun handleSocks(sock: Socket, host: String, port: Int) {
        stats["streams"]!!.incrementAndGet()
        val sid = mux.openStream(host, port)
        log("[client] stream $sid -> $host:$port")
        sockets[sid] = sock

        // uplink reader (owns socket rx side)
        Thread({
            try {
                val input = sock.getInputStream()
                val buf = ByteArray(65536)
                while (!stopFlag) {
                    val n = input.read(buf)
                    if (n < 0) break
                    stats["bytes_out"]!!.addAndGet(n.toLong())
                    mux.write(sid, buf.copyOf(n))
                }
            } catch (_: Exception) {
            } finally {
                mux.closeStream(sid)
            }
        }, "zp-up-$sid").apply { isDaemon = true; start() }

        // downlink drainer (owns socket tx side)
        try {
            val output = sock.getOutputStream()
            while (!stopFlag) {
                val data = mux.recv(sid, 500)
                if (data == null) {
                    if (!mux.streams.containsKey(sid)) break
                    continue
                }
                output.write(data)
                output.flush()
            }
        } catch (_: Exception) {
        } finally {
            mux.closeStream(sid)   // idempotent (v2)
            try { sock.shutdownInput() } catch (_: Exception) {}
            try { sock.shutdownOutput() } catch (_: Exception) {}
            try { sock.close() } catch (_: Exception) {}
            sockets.remove(sid)
            log("[client] stream $sid closed")
        }
    }

    companion object {
        fun loadConfig(json: String): JSONObject = JSONObject(json)
        fun decodeB64(s: String): ByteArray = java.util.Base64.getUrlDecoder().decode(s)
    }
}
