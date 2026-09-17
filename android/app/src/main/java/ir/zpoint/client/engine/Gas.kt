package ir.zpoint.client.engine

import android.util.Base64
import org.json.JSONArray
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URI
import java.net.URL
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.math.min

/**
 * Google Apps Script transport for Zpoint — Kotlin port of core/gas.py v1.
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 *
 * Mirrors gas/Code.gs:
 *   POST /exec?k=<token>&dir=up|dn   {"v":1,"f":[item,...]}
 *   GET  /exec?k=<token>&mode=up|dn&after=<c>&wait=<s>[&resync=1]
 *     -> {"ok":true,"b":[{"c":n,"f":[item,...]},...]}
 *
 * item = {"id": envId, "w": workerId, "n": workerSeq, "b": b64(envCt)}
 * env payload (sealed): {"v":1,"e":epoch,"f":[frame,...]}
 */
object GasCodec {
    const val MAX_FRAMES = 128
    const val BATCH_WIRE_CAP = 96 * 1024
    val ENV_LABEL_UP = byteArrayOf(0, 0, 'g'.code.toByte(), 'U'.code.toByte())
    val ENV_LABEL_DN = byteArrayOf(0, 0, 'g'.code.toByte(), 'D'.code.toByte())
    val ENV_AAD = "zpoint-batch-v1".toByteArray()

    fun b64e(b: ByteArray): String =
        Base64.encodeToString(b, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)

    /** PADDED-tolerant urlsafe decode: accept both NO_PADDING (Kotlin
     *  twin) and padded (Python seal_item) encodings. */
    fun b64d(s: String): ByteArray =
        Base64.decode(s + "=", Base64.URL_SAFE or Base64.NO_WRAP)

    fun sealItem(frames: List<Frame>, envId: Long, env: FrameCrypto,
                 wid: Int, wseq: Int, epoch: Long): Pair<JSONObject, Int> {
        // BUGFIX #23: stamp every envelope with its creation time so the
        // receiver can drop a cold-start replay of the 6h Apps Script ring.
        // This timestamp is only ever compared against OTHER envelopes from
        // the same sender -- never against the receiver's own clock -- so
        // phone/server clock skew is irrelevant.
        val payload = JSONObject().put("v", 1).put("e", epoch)
            .put("t", System.currentTimeMillis())
        val arr = JSONArray()
        frames.forEach { arr.put(
            JSONObject().put("i", it.i).put("s", it.s)
                .put("t", it.t).put("b", it.b)) }
        payload.put("f", arr)
        val ct = env.sealRaw(envId, payload.toString().toByteArray(), ENV_AAD)
        val item = JSONObject().put("id", envId).put("w", wid).put("n", wseq)
            .put("b", b64e(ct))
        return Pair(item, payload.toString().toByteArray().size)
    }

    fun openItem(item: JSONObject, env: FrameCrypto): JSONObject? = try {
        JSONObject(String(env.openRaw(b64d(item.getString("b")),
                                      item.getLong("id"), ENV_AAD)))
    } catch (_: Exception) { null }
}

/**
 * HTTP carrier for ONE deployed Web App (java.net only, follows the
 * /exec -> script.googleusercontent.com redirect chain explicitly).
 */
class GasCarrier(val url: String, private val token: String, val name: String) {
    var requests = AtomicLong(0); var errors = AtomicLong(0)
    var bytesUp = AtomicLong(0); var bytesDown = AtomicLong(0)
    private val postBase: String = url.trimEnd('/').substringBeforeLast("/exec") + "/exec"

    private fun open(method: String, urlStr: String, body: ByteArray? = null,
                     timeoutMs: Int = 70_000): HttpURLConnection {
        val c = URL(urlStr).openConnection() as HttpURLConnection
        c.requestMethod = method
        c.connectTimeout = 15_000
        c.readTimeout = timeoutMs
        // BUGFIX (the "ping OK but no data" bug, FINAL form): Google's flow
        // is  POST hop1 (body, executes doPost) -> 302 -> GET hop2 (result).
        // - streaming modes (fixed/chunked) break the chain with
        //   HttpRetryException / 411 / 405 respectively;
        // - buffered mode w/o explicit length sends no Content-Length -> 411.
        // So: followRedirects OFF + MANUAL chain: hop1 POST with exact
        // Content-Length, hop2 GET the Location (result).  Verified from
        // python: marker frame lands in the ring through this exact path.
        c.instanceFollowRedirects = false
        if (body != null) {
            c.doOutput = true
            c.setFixedLengthStreamingMode(body.size)
            c.setRequestProperty("Content-Type", "application/json")
        }
        c.setRequestProperty("User-Agent", "ZpointDroid/1.0 (research; @ily_bio)")
        return c
    }

    private fun call(method: String, urlStr: String, body: ByteArray? = null,
                     timeoutMs: Int = 70_000): JSONObject {
        requests.incrementAndGet()
        var u = urlStr
        var pendingBody = body
        var pendingMethod = method
        repeat(5) {
            val c = open(pendingMethod, u, pendingBody, timeoutMs)
            try {
                // write the body BEFORE reading the response code — with
                // fixed-length streaming the request is only sent once the
                // output stream is written & closed ("insufficient data
                // written" = responseCode read before the body was flushed)
                if (pendingBody != null) {
                    c.outputStream.write(pendingBody)
                    c.outputStream.flush()
                    c.outputStream.close()
                }
                val code = c.responseCode
                if (code in 301..308) {
                    val loc = c.getHeaderField("Location")
                        ?: throw RuntimeException("redirect w/o Location")
                    u = URI(u).resolve(loc).toString()
                    // 302 after a body-carrying POST: the POST already
                    // executed at hop1 — hop2 is a plain GET for the result.
                    if (pendingBody != null) {
                        pendingBody = null
                        pendingMethod = "GET"
                    }
                    c.disconnect()
                    return@repeat
                }
                if (code != 200) throw RuntimeException("HTTP $code")
                val data = c.inputStream.readBytes()
                bytesDown.addAndGet(data.size.toLong())
                return JSONObject(String(data, Charsets.UTF_8))
            } catch (e: Exception) {
                errors.incrementAndGet()
                throw e
            } finally {
                c.disconnect()
            }
        }
        throw RuntimeException("too many redirects")
    }

    private fun q(extra: Map<String, String>): String {
        val p = mutableListOf("k=" + URLEncoder.encode(token, "UTF-8"))
        extra.forEach { (k, v) -> p += "$k=" + URLEncoder.encode(v, "UTF-8") }
        return p.joinToString("&")
    }

    fun postItems(direction: String, items: List<JSONObject>): JSONObject =
        call("POST", "$postBase?${q(mapOf("dir" to direction))}",
             ("{\"v\":1,\"f\":" + JSONArray(items).toString() + "}")
                 .toByteArray().also { bytesUp.addAndGet(it.size.toLong()) })

    fun poll(mode: String, after: Long, waitS: Int, resync: Boolean = false):
            JSONObject {
        val m = linkedMapOf("mode" to mode, "after" to after.toString(),
                            "wait" to waitS.toString())
        if (resync) m["resync"] = "1"
        return call("GET", "$postBase?${q(m)}", null,
                    (waitS + 20) * 1000)
    }

    fun ping(): Long {
        val t0 = System.currentTimeMillis()
        val r = call("GET", "$postBase?${q(mapOf("mode" to "ping"))}", null, 15_000)
        if (!r.optBoolean("ok")) throw RuntimeException("bad ping reply")
        return System.currentTimeMillis() - t0
    }
}

/** Per-worker (w,n) reorder + id dedupe — Kotlin twin of ItemAssembler. */
class GasAssembler(private val deliver: (JSONObject) -> Unit,
                   private val log: (String) -> Unit,
                   private val holdMs: Long = 1000) {
    private val lock = Object()
    private val seenIds = HashSet<Long>()
    private val expected = HashMap<Int, Int>()
    private val hold = HashMap<Int, HashMap<Int, Pair<Long, JSONObject>>>()
    var stats = ConcurrentHashMap<String, AtomicLong>().apply {
        listOf("items_in", "dupes", "held_flushes", "stale_epoch")
            .forEach { put(it, AtomicLong(0)) }
    }
    @Volatile private var stopped = false

    fun entry(item: JSONObject) {
        val eid = try { item.getLong("id") } catch (_: Exception) { return }
        val wid = item.optInt("w", 0)
        val n = item.optInt("n", 0)
        val ready = ArrayList<JSONObject>()
        synchronized(lock) {
            if (expected[wid]?.let { n < it } == true ||
                hold[wid]?.containsKey(n) == true) {
                stats["dupes"]!!.incrementAndGet(); return
            }
            if (eid in seenIds) { stats["dupes"]!!.incrementAndGet(); return }
            val exp = expected[wid] ?: 1
            if (n == exp) {
                seenIds.add(eid)
                ready.add(item)
                expected[wid] = n + 1
            } else {
                hold.getOrPut(wid) { HashMap() }[n] = Pair(now(), item)
            }
            expected[wid]?.let { e0 ->
                hold[wid]?.let { h ->
                    var nxt = e0
                    while (h.containsKey(nxt)) {
                        val it2 = h.remove(nxt)!!.second
                        seenIds.add(it2.getLong("id"))
                        ready.add(it2)
                        nxt++
                    }
                    expected[wid] = nxt
                }
            }
        }
        ready.forEach(deliver)
    }

    /** Flush holds whose hold expired — Kotlin twin of the core #15 fix:
     *  the old check `h[exp] ?: continue` was DEAD (exp is the MISSING
     *  seq — it is never in h), so out-of-order streams stalled forever.
     *  Correct: after holdMs, flush the contiguous run starting at the
     *  LOWEST available n; fresh items (their own hold not yet expired)
     *  are re-parked for the next sweep. */
    fun sweep() {
        val flush = ArrayList<JSONObject>()
        val repark = ArrayList<JSONObject>()
        val nowT = now()
        synchronized(lock) {
            if (stopped) return
            for ((wid, h) in hold) {
                if (h.isEmpty()) continue
                val oldest = h.values.minOf { it.first }
                if (nowT - oldest < holdMs) continue          // still holding
                val lowest = h.keys.min()
                var nxt = lowest
                while (h.containsKey(nxt)) {
                    val entry = h.remove(nxt)!!
                    if (nowT - entry.first < holdMs) {
                        repark.add(entry.second)               // arrived early
                    } else {
                        seenIds.add(entry.second.getLong("id"))
                        flush.add(entry.second)
                    }
                    nxt++
                }
                expected[wid] = nxt   // first undelivered n — stream resumes
                stats["held_flushes"]!!.incrementAndGet()
                log("[gas] worker $wid: hold expired — flushing from n=$lowest " +
                    "(${flush.size} item(s); loss possible at the gap)")
            }
        }
        synchronized(lock) {
            repark.forEach { item ->
                val w = item.optInt("w", 0)
                val n = item.optInt("n", 0)
                hold.getOrPut(w) { HashMap() }[n] = Pair(nowT, item)
            }
        }
        flush.forEach(deliver)
    }

    fun startSweeper() {
        thread(name = "zp-gas-sweep") {
            while (!stopped) {
                try { Thread.sleep(holdMs); sweep() } catch (_: InterruptedException) { return@thread }
            }
        }
    }

    fun stop() { synchronized(lock) { stopped = true } }

    /** Generation reset (core BUGFIX #18 parity): the sender restarted
     *  and began numbering from n again while we still expected the OLD
     *  counter — without this reset the whole worker lane dies silently
     *  ("ping OK, zero traffic" after any app restart). */
    fun resetWorker(wid: Int, n: Int) {
        synchronized(lock) {
            hold[wid]?.clear()
            expected[wid] = n
        }
    }

    private fun now() = System.currentTimeMillis()
}

/** Long-poll downloader: one thread per script of the pool. */
class GasDownloader(urls: List<String>, private val token: String,
                    private val env: FrameCrypto, private val mode: String,
                    private val onFrame: (Frame) -> Unit,
                    private val log: (String) -> Unit) {
    val carriers = urls.distinct().mapIndexed { i, u ->
        GasCarrier(u, token, "gas$i") }
    private val asm = GasAssembler({ item -> onItem(item) }, log)
    @Volatile var stopped = false
    var stats = ConcurrentHashMap<String, AtomicLong>().apply {
        put("batches_in", AtomicLong(0)); put("opens_failed", AtomicLong(0))
        // BUGFIX #26: onItem() does stats["stale_epoch"]!!.incrementAndGet(),
        // but this map never contained that key -- so the FIRST stale
        // envelope threw a NullPointerException on the `!!`. It was raised
        // inside the poll thread, caught by loop()'s generic handler, logged
        // as "poll error: null" and slept off for 1.5s, silently discarding
        // the remainder of that batch. A crash disguised as a network hiccup.
        put("stale_epoch", AtomicLong(0)); put("stale_age", AtomicLong(0))
    }
    val threads = ArrayList<Thread>()

    fun start() {
        carriers.forEach { car ->
            val t = thread(name = "zp-gas-poll-${car.name}") { loop(car) }
            threads.add(t)
        }
        asm.startSweeper()
    }

    fun stopAll() {
        stopped = true
        asm.stop()
        carriers.forEach { c ->
            try { URL(c.url).openConnection().connectTimeout = 1 } catch (_: Exception) {}
        }
        threads.forEach { t ->
            // poll threads block in HTTP read; they are daemons and exit on
            // the next iteration check — do NOT join (network read blocks).
            t.interrupt()
        }
    }

    private fun loop(car: GasCarrier) {
        var after = -1L
        var first = true
        while (!stopped) {
            try {
                val wasFirst = first
                val reply = if (first) {
                    val r = car.poll(mode, after, 0, resync = true); first = false; r
                } else car.poll(mode, after, 50)
                if (!reply.optBoolean("ok")) throw RuntimeException(reply.optString("err"))
                val batches = reply.optJSONArray("b") ?: continue
                // BUGFIX #23: arm the age floor from this resync drain BEFORE
                // a single one of its items reaches the assembler.
                if (wasFirst && batches.length() > 0) armStaleFloor(batches, car.name)
                if (batches.length() > 0) {
                    for (i in 0 until batches.length()) {
                        val b = batches.getJSONObject(i)
                        val c = b.getLong("c")
                        if (c > after) after = c
                        stats["batches_in"]!!.incrementAndGet()
                        val items = b.optJSONArray("f") ?: continue
                        for (j in 0 until items.length()) {
                            val it = items.optJSONObject(j) ?: continue
                            asm.entry(it)
                        }
                    }
                }
            } catch (e: Exception) {
                if (!stopped) log("[gas] ${car.name} poll error: ${e.message}")
                try { Thread.sleep(1500) } catch (_: InterruptedException) { return }
            }
        }
    }

    private fun onItem(item: JSONObject) {
        val payload = GasCodec.openItem(item, env)
        if (payload == null) {
            stats["opens_failed"]!!.incrementAndGet(); return
        }
        val ep = payload.optLong("e", 0)
        // BUGFIX #23: age gate -- see armStaleFloor() for the full rationale.
        val ts = payload.optLong("t", 0)
        if (ts > 0) {
            synchronized(floorLock) {
                if (ts < staleBefore) {
                    stats["stale_age"]!!.incrementAndGet(); return
                }
                val floor = ts - MAX_ENV_LAG_MS
                if (floor > staleBefore) staleBefore = floor
            }
        }
        // epoch gate: drop pre-start replays (ratchet floor = max seen - 1);
        // a NEW sender generation (higher epoch) restarts its (w, n)
        // numbering — rebase the reorder buffer so the lane resumes
        // (parity with the core BUGFIX #18).
        synchronized(asm) {
            val wid = item.optInt("w", 0)
            val prev = epochByWorker[wid] ?: 0L
            if (ep < epochFloor) { stats["stale_epoch"]!!.incrementAndGet(); return }
            if (ep > prev) {
                epochByWorker[wid] = ep
                if (prev == 0L) epochFloor = maxOf(epochFloor, ep - 1)
                else asm.resetWorker(wid, item.optInt("n", 0))
            }
        }
        val frames = payload.optJSONArray("f") ?: return
        for (i in 0 until frames.length()) {
            val f = frames.getJSONObject(i)
            // epoch-stamp every frame BEFORE handing it down: the node's
            // dedupe key must be (epoch, seq) — a restarted phone re-numbers
            // its frames from seq=1 (parity with core BUGFIX #20).
            // BUGFIX #25: the epoch was written into a throwaway JSON key
            // ("_zp_ep") that nothing ever read, and then DROPPED -- Frame was
            // constructed WITHOUT it, so Frame.ep stayed 0 forever and
            // GasClientNode.dedupeKey() degenerated to (0, seq). After an exit
            // restart the new generation re-numbers its transport seqs from 1
            // and collided with (0,1)..(0,n) already sitting in `seen`, so
            // every frame of the new generation was discarded as a duplicate:
            // "ping OK, zero traffic" until the app data was cleared.
            // The `ep` field existed on Frame -- it was simply never filled.
            onFrame(Frame(f.optLong("i", 0), f.optInt("s", 0),
                          f.optString("t", "d"), f.optString("b", ""), ep))
        }
    }

    private val epochByWorker = HashMap<Int, Long>()
    @Volatile private var epochFloor = 0L
    @Volatile private var staleBefore = 0L
    private val floorLock = Object()

    /**
     * BUGFIX #23: arm the age floor from the NEWEST envelope in the first
     * resync reply -- not the oldest.
     *
     * The first poll is `after=-1&resync=1`, which replays up to 6 hours of
     * the Apps Script ring. Those envelopes decrypt perfectly (same session
     * key), so they were injected as live traffic into freshly allocated
     * stream ids and additionally poisoned the dedupe set, which then
     * rejected the genuine frames that followed.
     *
     * The pre-existing epoch gate was supposed to stop this, but it armed
     * `epochFloor` from the FIRST item it happened to see -- i.e. the OLDEST
     * stale envelope in the drain -- so the floor landed below the entire
     * backlog and let all of it through.
     *
     * Simply skipping the first batch is also wrong: an exit that subscribes
     * slightly late legitimately needs that backlog. So we keep everything
     * within MAX_ENV_LAG_MS of the newest envelope we can see and drop only
     * what is older than that.
     */
    private fun armStaleFloor(batches: JSONArray, carName: String) {
        var newest = 0L
        for (i in 0 until batches.length()) {
            val items = batches.optJSONObject(i)?.optJSONArray("f") ?: continue
            for (j in 0 until items.length()) {
                val it = items.optJSONObject(j) ?: continue
                val p = GasCodec.openItem(it, env) ?: continue
                val t = p.optLong("t", 0)
                if (t > newest) newest = t
            }
        }
        // newest == 0 means the sender predates #23 and stamps no timestamp:
        // stay disabled rather than dropping everything from an old peer.
        if (newest <= 0L) return
        synchronized(floorLock) {
            val floor = newest - MAX_ENV_LAG_MS
            if (floor > staleBefore) {
                staleBefore = floor
                log("[gas] $carName age floor armed: dropping envelopes " +
                    "older than $floor (newest in ring $newest)")
            }
        }
    }

    companion object {
        /** Tolerated envelope age, relative to the newest envelope seen from
         *  the same sender. Generous enough for a slow pool round trip and a
         *  requeue after a failover, far below the ring's 6h TTL. */
        const val MAX_ENV_LAG_MS = 120_000L
    }
}

/** Outbound pool: lane->worker sharding + home-pinned failover. */
class GasPool(urls: List<String>, private val token: String,
              private val env: FrameCrypto, private val dir: String,
              private val log: (String) -> Unit,
              workers: Int = 4) {
    val carriers = urls.distinct().mapIndexed { i, u ->
        GasCarrier(u, token, "gas$i") }
    val stats = ConcurrentHashMap<String, AtomicLong>().apply {
        put("batches_out", AtomicLong(0)); put("frames_out", AtomicLong(0))
    }
    private val epoch = System.currentTimeMillis() and 0x7FFF_FFFF_FFFF_FFFF
    private val envIdSeq = AtomicLong(epoch)
    private val laneMap = ConcurrentHashMap<String, Int>()
    private val nextW = AtomicLong(0)
    private val cool = ConcurrentHashMap<Int, Long>()
    private val nW = workers.coerceIn(1, 8)
    private val queues = Array(nW) { ArrayDeque<Pair<String, Frame>>() }
    private val qlcks = Array(nW) { Object() }
    // BUGFIX (core parity): worker id + worker_seq are PER-WORKER.  The old
    // code stamped every envelope with wid=0 and a POOL-wide wseq, so the
    // receiver's (w,n) reorder buffer saw one fake worker whose n jumped
    // around — out-of-order frames parked forever in the hold buffer.
    private val wseq = IntArray(nW)
    private val stop = java.util.concurrent.atomic.AtomicBoolean(false)

    init {
        for (w in queues.indices) {
            thread(name = "zp-gas-w$w") { runWorker(w) }
        }
    }

    private fun workerFor(lane: String): Int {
        return laneMap.getOrPut(lane) { (nextW.getAndIncrement() % queues.size).toInt() }
    }

    fun submit(frame: Frame, lane: String) {
        if (stop.get()) return
        val w = workerFor(lane)
        synchronized(qlcks[w]) {
            queues[w].addLast(lane to frame)
            (qlcks[w] as Object).notifyAll()
        }
    }

    private fun runWorker(w: Int) {
        val buf = ArrayList<Pair<String, Frame>>()
        while (!stop.get()) {
            synchronized(qlcks[w]) {
                if (queues[w].isEmpty())
                    (qlcks[w] as Object).wait(120)
                while (queues[w].isNotEmpty()) buf.add(queues[w].removeFirst())
            }
            if (buf.isNotEmpty()) {
                ship(w, ArrayList(buf)); buf.clear()
            }
        }
    }

    private fun ship(w: Int, items: List<Pair<String, Frame>>) {
        val frames = items.map { it.second }
        val envId = envIdSeq.incrementAndGet()
        val wseqV = ++wseq[w]          // per-worker, gapless per lane contract
        // BUGFIX (core #16 parity): wire-cap overflow frames are REQUEUED
        // at the front of this worker's queue (order preserved) instead of
        // silently dropped — zero data loss.
        var list = frames
        var sealed = GasCodec.sealItem(list, envId, env, w, wseqV, epoch)
        var item = sealed.first
        var size = sealed.second
        val overflow = ArrayList<Pair<String, Frame>>()
        while (size > GasCodec.BATCH_WIRE_CAP && list.size > 1) {
            list = list.dropLast(1)
            sealed = GasCodec.sealItem(list, envId, env, w, wseqV, epoch)
            item = sealed.first; size = sealed.second
            overflow.add(0, items[list.size])      // keep original lane order
        }
        if (overflow.isNotEmpty()) {
            log("[gas] wire-cap: envelope $envId carries ${list.size}/${frames.size} " +
                "frame(s); ${overflow.size} requeued")
        }
        val home = carriers[w % carriers.size]
        val order = listOf(home) + carriers.filterNot { it === home }
        for ((attempt, car) in order.withIndex()) {
            try {
                val reply = car.postItems(dir, listOf(item))
                if (reply.optBoolean("ok")) {
                    stats["batches_out"]!!.incrementAndGet()
                    stats["frames_out"]!!.addAndGet(list.size.toLong())
                    requeue(w, overflow)
                    return
                }
                markFail(car, reply.optString("err"))
            } catch (e: Exception) {
                markFail(car, e.message ?: e.javaClass.simpleName)
            }
            try { Thread.sleep(min(500L, 150L * (attempt + 1))) } catch (_: InterruptedException) { return }
        }
        log("[gas] envelope $envId UNDELIVERED (${list.size} frames) — pool exhausted")
        requeue(w, overflow)   // undelivered → back on the queue, zero loss
    }

    private fun requeue(w: Int, overflow: List<Pair<String, Frame>>) {
        if (overflow.isEmpty()) return
        synchronized(qlcks[w]) {
            for (i in overflow.indices) queues[w].add(i, overflow[i])
            (qlcks[w] as Object).notifyAll()
        }
    }

    fun stopAll() { stop.set(true) }
    private fun markFail(car: GasCarrier, why: String) {
        log("[gas] ${car.name} failing over: $why")
    }
}
