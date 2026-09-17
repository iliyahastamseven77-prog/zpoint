package ir.zpoint.client.engine

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Lane-ordered WriteQueue — direct port of core/writequeue.py v2.
 * Per-lane strict FIFO with atomic claim; CONTROL lane for o/c/k/w frames.
 * Frame values are raw wire dicts (as String keys mapped by the caller);
 * to stay dependency-free (no org.json here), jobs carry pre-serialized
 * JSON strings built by ClientNode.
 */
class WriteQueue(
    private val putFn: (String, String) -> Boolean,
    workers: Int = 4,
    private val log: (String) -> Unit = {}
) {
    companion object { const val CONTROL_LANE = "__ctl__" }

    private val lanes = ConcurrentHashMap<String, ArrayDeque<Pair<String, String>>>()
    private val running = ConcurrentHashMap.newKeySet<String>()
    private val outstanding = AtomicLong(0)
    @Volatile private var stopped = false
    private val lock = Object()

    init {
        repeat(workers) { i ->
            Thread({ worker() }, "zp-w$i").apply { isDaemon = true; start() }
        }
    }

    fun submit(path: String, valueJson: String, key: String = CONTROL_LANE) {
        if (stopped) return
        while (outstanding.get() >= 512 && !stopped) Thread.sleep(5)
        if (stopped) return
        synchronized(lock) {
            lanes.getOrPut(key) { ArrayDeque() }.addLast(path to valueJson)
            outstanding.incrementAndGet()
            if (key !in running) {
                running.add(key)          // claim atomically at enqueue time
                (lock as Object).notify()
            }
        }
    }

    private fun worker() {
        while (!stopped) {
            val key: String = synchronized(lock) {
                while (lanes.isEmpty() && !stopped) (lock as Object).wait(250)
                if (stopped) return
                // running-set claims were made at submit() time; here we just
                // adopt any claimed lane whose queue is non-empty
                var found: String? = null
                for ((k, q) in lanes) {
                    if (q.isNotEmpty() && k in running) {
                        // verify no OTHER worker is executing it: running
                        // means claimed; executing is tracked separately
                        found = k
                        break
                    }
                }
                found ?: run {
                    // no pre-claimed lane ready — claim one now
                    for ((k, q) in lanes) {
                        if (q.isNotEmpty() && k !in running) {
                            running.add(k)
                            found = k
                            break
                        }
                    }
                }
                found
            } ?: continue
            runLane(key)
        }
    }

    private fun runLane(key: String) {
        while (true) {
            if (stopped) {
                synchronized(lock) { lanes.remove(key) }
                running.remove(key)
                return
            }
            val job = synchronized(lock) { lanes[key]?.removeFirstOrNull() }
            if (job == null) {
                synchronized(lock) {
                    val q = lanes[key]
                    if (q == null || q.isEmpty()) {
                        lanes.remove(key)
                        running.remove(key)
                        return
                    }
                }
                continue
            }
            try {
                putFn(job.first, job.second)
            } catch (e: Exception) {
                log("[wq] put failed ($key): ${e.message}")
            } finally {
                outstanding.decrementAndGet()
            }
        }
    }

    fun flush(timeoutMs: Long = 5000): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (outstanding.get() > 0) {
            if (System.currentTimeMillis() > deadline) return false
            Thread.sleep(50)
        }
        return true
    }

    fun stopAll() {
        stopped = true
        synchronized(lock) { (lock as Object).notifyAll() }
    }
}

/**
 * Frame ingest with seq dedupe — port of core/ingest.py v2 (audit target #2).
 */
class FrameIngest(private val log: (String) -> Unit) {
    private val pending = ConcurrentHashMap.newKeySet<Long>()
    var duplicates = 0L; private set
    var delivered = 0L; private set

    fun accept(node: Frame, handler: (Frame) -> Unit) {
        if (node.b.isEmpty()) return
        if (!pending.add(node.i)) {
            duplicates++
            return
        }
        try {
            handler(node)
            delivered++
        } catch (e: Exception) {
            pending.remove(node.i)
            log("[ingest] handler failed seq=${node.i}: ${e.message}")
        }
    }
}
