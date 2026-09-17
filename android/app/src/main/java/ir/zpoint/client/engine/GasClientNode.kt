package ir.zpoint.client.engine

import org.json.JSONObject
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

/**
 * Zpoint client node over the Google Apps Script pool — Kotlin twin of
 * GASClientNode in core/gas_nodes.py (v3).
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 *
 *  apps -> SOCKS5(:port) -> mux frames -> sealed batches -> GAS pool
 *  exit long-polls the uplink; this node long-polls the downlink (dn).
 *
 * Ingest threading mirrors the v2 audit: ONE ingest thread, socket
 * lifecycle owned by the drain loop, T_CLOSE exactly once, control
 * frames (w/c/o) APPLIED (the #12/#13/#14 class of bugs is fixed here
 * the same way it was fixed in core).
 */
class GasClientNode(
    urls: List<String>,
    private val token: String,
    private val sessionId: String,
    key: ByteArray,
    upPrefix: ByteArray,
    downPrefix: ByteArray,
    private val clientLabel: String = "c1",
    private val listenPort: Int = 1086,
    private val log: (String) -> Unit = {}
) {
    private val cryptoUp = FrameCrypto(key, upPrefix)
    private val cryptoDown = FrameCrypto(key, downPrefix)
    private val envUp = FrameCrypto(key, GasCodec.ENV_LABEL_UP)
    private val envDn = FrameCrypto(key, GasCodec.ENV_LABEL_DN)
    private val mux = Mux({ f -> sendUp(f) }, cryptoUp, isServer = false,
                          clientLabel = clientLabel)
    private val pool = GasPool(urls, token, envUp, "up", log)
    private val downloader = GasDownloader(urls, token, envDn, "dn",
                                           { f -> onFrame(f) }, log)

    val stats = ConcurrentHashMap<String, AtomicLong>().apply {
        listOf("frames_in", "frames_out", "bytes_in", "bytes_out", "streams", "dupes")
            .forEach { put(it, AtomicLong(0)) }
    }
    @Volatile private var stopFlag = false
    private val socksSockets = ConcurrentHashMap<Int, Socket>()
    private var srv: ServerSocket? = null
    private var ingestThread: Thread? = null
    private val rxq = java.util.concurrent.ArrayBlockingQueue<Frame>(2048)

    // ---------- BUGFIX #22: real end-to-end liveness handshake ----------
    // start() below performs ZERO network I/O -- it binds a loopback
    // ServerSocket and spawns poll threads. EngineBus used to flip straight to
    // CONNECTED once it returned, so a wrong token, wrong gas_urls, a revoked
    // deployment, or simply no exit node running all looked identical to
    // success. mode=ping is no substitute: it proves the Apps Script is alive
    // and says nothing about the exit sitting behind it. The only honest proof
    // of an end-to-end path is a nonce WE generated coming back to us sealed
    // with the session key.
    private val hsPending = ConcurrentHashMap<Long, Long>()   // nonce -> sentAt
    private val hsLock = Object()
    @Volatile var handshakeOk = false
        private set
    @Volatile var rttMs = 0L
        private set
    @Volatile var exitInfo = ""
        private set
    @Volatile private var hsLastAck = 0L
    private var hbThread: Thread? = null

    private fun sendHello() {
        // BUGFIX #27: a RANDOM 62-bit nonce, never a counter. A counter
        // restarts at 1 on every launch, so a replayed ack from a previous run
        // -- or an ack genuinely meant for a different client on the same
        // session -- matched and "proved" a liveness that did not exist.
        val nonce = java.util.concurrent.ThreadLocalRandom.current()
            .nextLong(1L, Long.MAX_VALUE / 2)
        synchronized(hsLock) {
            // bounded: a long connecting phase must not grow this forever
            if (hsPending.size > 16) hsPending.clear()
            hsPending[nonce] = System.currentTimeMillis()
        }
        val js = JSONObject()
            .put("hello", 1)
            .put("n", nonce)
            .put("cid", clientLabel)
            .put("sess", sessionId)
            .put("ver", HS_VERSION)
            .put("ts", System.currentTimeMillis())
            .toString()
        mux.sendHello(js)
    }

    private fun onHelloAck(body: String) {
        val o = try { JSONObject(body) } catch (_: Exception) { return }
        if (o.optInt("ack", 0) != 1) return          // our own hello echoed back
        // BUGFIX #27: an ack only counts if it answers a nonce THIS node still
        // has outstanding AND is addressed to THIS client id. Without the cid
        // check, two phones sharing a session would each accept the other's
        // ack and both report a working tunnel while only one had one.
        val ackCid = o.optString("cid", "")
        if (ackCid.isNotEmpty() && ackCid != clientLabel) {
            log("[gas-client] ignoring ack addressed to '$ackCid'")
            return
        }
        val nonce = o.optLong("n", 0)
        val sentAt = synchronized(hsLock) { hsPending.remove(nonce) } ?: return
        rttMs = System.currentTimeMillis() - sentAt
        hsLastAck = System.currentTimeMillis()
        exitInfo = "sess=${o.optString("sess")} up=${o.optLong("up")}s " +
                   "streams=${o.optInt("streams")}"
        handshakeOk = true
        log("[gas-client] handshake ok - rtt ${rttMs}ms - $exitInfo")
    }

    /**
     * Block until the exit answers, or throw.
     *
     * Re-probes every 3s rather than sending one hello and waiting: a single
     * hello can legitimately be lost in a pool failover, or land in an
     * envelope that overflows BATCH_WIRE_CAP and gets requeued.
     *
     * NEVER call this from the main thread -- it is a relay round trip.
     */
    fun handshake(timeoutMs: Long = 45_000): Long {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (stopFlag) throw java.io.IOException("stopped")
            sendHello()
            val slice = System.currentTimeMillis() + 3_000
            while (System.currentTimeMillis() < slice) {
                if (handshakeOk) return rttMs
                try { Thread.sleep(50) } catch (_: InterruptedException) {
                    throw java.io.IOException("interrupted")
                }
            }
        }
        throw java.io.IOException(
            "exit did not answer the handshake - the script pool is " +
            "reachable but no exit node is alive on this session/key")
    }

    /**
     * Keep proving the path is alive. Without this, a tunnel that dies
     * mid-session (exit process killed, token rotated, deployment replaced)
     * kept showing a green "Connected" until the user tried to load a page.
     */
    private fun heartbeatLoop() {
        while (!stopFlag) {
            try { Thread.sleep(20_000) } catch (_: InterruptedException) { return }
            if (stopFlag) return
            if (!handshakeOk) continue        // EngineBus owns the retry
            sendHello()
            if (System.currentTimeMillis() - hsLastAck > 75_000) {
                handshakeOk = false
                log("[gas-client] exit silent >75s - handshake lost")
            }
        }
    }

    private fun sendUp(f: Frame) {
        // ALL frames of one stream share ONE lane (u:sid) — OPEN precedes
        // DATA deterministically and worker_seq stays gapless.
        pool.submit(f, "u:${f.s}")
        stats["frames_out"]!!.incrementAndGet()
    }

    private fun onFrame(f: Frame) {
        if (!rxq.offer(f)) log("[gas-client] rx queue full — frame dropped")
    }

    private val seen = java.util.concurrent.ConcurrentHashMap.newKeySet<Pair<Long, Long>>()

    fun start() {
        val s = ServerSocket()
        s.reuseAddress = true
        s.bind(java.net.InetSocketAddress(
            InetAddress.getByName("127.0.0.1"), listenPort), 64)
        srv = s
        Socks5.serve(s, { stopFlag },
            { sock, host, port -> handleSocks(sock, host, port) })
        ingestThread = thread(name = "zp-gas-cli-ingest") { ingestLoop() }
        downloader.start()
        // BUGFIX #22: liveness heartbeat (see heartbeatLoop).
        hbThread = thread(name = "zp-gas-hb", isDaemon = true) { heartbeatLoop() }
        log("[gas-client] SOCKS5 on 127.0.0.1:$listenPort " +
            "session=$sessionId pool=${pool.carriers.size}")
    }

    fun stopAll() {
        stopFlag = true
        handshakeOk = false
        synchronized(hsLock) { hsPending.clear() }
        hbThread?.interrupt()
        mux.closed = true
        downloader.stopAll()
        pool.stopAll()
        try { srv?.close() } catch (_: Exception) {}
        socksSockets.values.forEach {
            try { it.shutdownInput() } catch (_: Exception) {}
            try { it.shutdownOutput() } catch (_: Exception) {}
            try { it.close() } catch (_: Exception) {}
        }
        socksSockets.clear()
    }

    // ---------- ingest (single thread) ----------
    private fun ingestLoop() {
        while (!stopFlag) {
            val f = try { rxq.poll(500, java.util.concurrent.TimeUnit.MILLISECONDS) }
                    catch (_: InterruptedException) { continue } ?: continue
            try {
                val key = dedupeKey(f)
                if (!seen.add(key)) { stats["dupes"]!!.incrementAndGet(); continue }
                val (sid, type, body) = Frames.unpack(f, cryptoDown)
                when (type) {
                    Frames.T_DATA -> {
                        stats["bytes_in"]!!.addAndGet(body.size.toLong())
                        muxDeliver(sid, body)
                        // BUGFIX (core #12 parity): the client MUST answer
                        // the exit's downlink with T_CREDIT grants — the
                        // old port silently dropped them, so any stream
                        // larger than one 512KB window froze.
                        // BUGFIX #24: this was
                        //   sendUp(Frame(0, sid, T_CREDIT, g.toString()))
                        // -- an UNSEALED frame carrying a plaintext body and
                        // transport seq 0, built by hand instead of through
                        // Frames.pack(). The exit dropped every one of them on
                        // AES-GCM InvalidTag, so its tx credits were never
                        // replenished. See Mux.sendCredit for the full story.
                        mux.grantsFor(sid, body.size.toLong()).forEach { g ->
                            mux.sendCredit(sid, g)
                        }
                    }
                    Frames.T_CREDIT -> mux.applyCredit(sid, String(body).toInt())
                    Frames.T_CLOSE -> mux.peerClosed(sid)
                    Frames.T_OPEN -> mux.peerOpened(sid)
                    // BUGFIX #22: the exit's handshake ack. Handled here
                    // because this node unpacks frames itself rather than
                    // routing them through Mux.deliver().
                    Frames.T_HELLO -> onHelloAck(String(body))
                }
                stats["frames_in"]!!.incrementAndGet()
            } catch (e: Exception) {
                log("[gas-client] bad frame: ${e.message}")
            }
        }
    }

    /** Dedupe key: (epoch, transport seq) — a restarted engine re-numbers
     *  its seqs from 1 with a NEW envelope epoch, so bare-seq dedupe
     *  would drop the whole new generation (core BUGFIX #20 parity). */
    private fun dedupeKey(f: Frame): Pair<Long, Long> {
        val gen = try { f.ep } catch (_: Exception) { 0L }
        return gen to f.i
    }

    private fun muxDeliver(sid: Int, body: ByteArray) {
        val st = mux.streams[sid]
        if (st != null) synchronized(st) {
            st.recvQueue.addLast(body)
            (st as Object).notifyAll()
        }
    }

    // ---------- SOCKS ----------
    private fun handleSocks(sock: Socket, host: String, port: Int) {
        stats["streams"]!!.incrementAndGet()
        val sid = mux.openStream(host, port)
        log("[client] stream $sid -> $host:$port")
        socksSockets[sid] = sock

        thread(name = "zp-up-$sid") {
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
        }

        try {
            val output = sock.getOutputStream()
            while (!stopFlag) {
                val data = mux.recv(sid, 500)
                if (data != null) {
                    output.write(data); output.flush()
                    continue
                }
                // null = timeout OR close — disambiguate before tearing the
                // stream down (BUGFIX: a T_CLOSE received while data was
                // still queued in the recv buffer dropped that buffered
                // tail — the classic "page almost loads then freezes").
                val st = mux.streams[sid]
                if (st == null || !st.open) {
                    // final drain: hand over anything still buffered
                    while (true) {
                        val tail = mux.recv(sid, 0) ?: break
                        output.write(tail); output.flush()
                    }
                    break
                }
            }
        } catch (_: Exception) {
        } finally {
            mux.closeStream(sid)
            try { sock.shutdownInput() } catch (_: Exception) {}
            try { sock.shutdownOutput() } catch (_: Exception) {}
            try { sock.close() } catch (_: Exception) {}
            socksSockets.remove(sid)
            log("[client] stream $sid closed")
        }
    }

    companion object {
        /** Handshake protocol version. Bump only on a breaking change to the
         *  hello/ack JSON shape; the exit refuses a mismatch explicitly
         *  instead of timing out with no explanation. */
        const val HS_VERSION = "zpoint-gas/1"

        /** Build from a transport=gas config JSON string. */
        fun fromConfig(json: String, listenPort: Int,
                       log: (String) -> Unit): GasClientNode {
            val c = JSONObject(json)
            require(c.optString("transport") == "gas") { "not a gas config" }
            val urls = mutableListOf<String>()
            val arr = c.getJSONArray("gas_urls")
            for (i in 0 until arr.length()) urls.add(arr.getString(i))
            require(urls.isNotEmpty()) { "gas_urls empty" }
            val dec = java.util.Base64.getUrlDecoder()
            return GasClientNode(
                urls, c.getString("gas_token"), c.getString("session"),
                dec.decode(c.getString("key")),
                dec.decode(c.getString("up_prefix")),
                dec.decode(c.getString("down_prefix")),
                clientLabel = c.optString("client_id", "c1"),
                listenPort = listenPort, log = log)
        }

        fun b64d(s: String): ByteArray = java.util.Base64.getUrlDecoder().decode(s)
    }
}
