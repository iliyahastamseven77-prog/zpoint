package ir.zpoint.client.engine

import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URI
import java.net.URL
import java.net.URLEncoder
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

/**
 * Firebase RTDB REST client + SSE reader — mirrors core/rtdb.py v2.
 * Pure java.net (no third-party deps).  Hardening carried over:
 *  - explicit redirect follow for PUT/GET/DELETE
 *  - SSE: follow 307 BEFORE parsing, on_ready() hook for gap-fill,
 *    connection closed on every exit path
 */
class Rtdb(dbUrl: String, private val auth: String) {

    init { require(dbUrl.startsWith("https://")) { "db_url must be https" } }
    private val base = dbUrl.trimEnd('/')

    private fun url(path: String, extra: String = ""): String {
        val p = "$base/${path.trim('/')}.json"
        val params = mutableListOf<String>()
        if (auth.isNotEmpty()) params += "auth=" + URLEncoder.encode(auth, "UTF-8")
        if (extra.isNotEmpty()) params += extra
        return p + if (params.isNotEmpty()) "?" + params.joinToString("&") else ""
    }

    private fun open(method: String, urlStr: String, body: ByteArray? = null):
            HttpURLConnection {
        val conn = URL(urlStr).openConnection() as HttpURLConnection
        conn.requestMethod = method
        conn.connectTimeout = 10_000
        conn.readTimeout = 30_000
        if (body != null) {
            conn.doOutput = true
            conn.setFixedLengthStreamingMode(body.size)
            conn.setRequestProperty("Content-Type", "application/json")
        }
        conn.setRequestProperty("User-Agent", "ZpointDroid/1.0 (research; @ily_bio)")
        return conn
    }

    private fun follow(method: String, path: String, body: ByteArray?): Pair<Int, ByteArray> {
        var url = url(path)
        repeat(5) {
            val c = open(method, url, body)
            try {
                val code = c.responseCode
                if (code in 301..308) {
                    val loc = c.getHeaderField("Location") ?: throw RuntimeException("redirect w/o Location")
                    url = URI(url).resolve(loc).toString()
                    c.disconnect()
                    return@repeat
                }
                val stream = if (code in 200..299) c.inputStream else c.errorStream
                val data = stream?.readBytes() ?: ByteArray(0)
                return Pair(code, data)
            } finally {
                c.disconnect()
            }
        }
        throw RuntimeException("too many redirects: $path")
    }

    fun put(path: String, value: JSONObject): Boolean {
        val (code, _) = follow("PUT", path, value.toString().toByteArray())
        return code == 200
    }

    fun get(path: String): JSONObject? {
        val (code, data) = follow("GET", path, null)
        if (code != 200) return null
        val s = String(data, StandardCharsets.UTF_8)
        if (s.isEmpty() || s == "null") return null
        return JSONObject(s)
    }

    fun delete(path: String): Boolean {
        val (code, _) = follow("DELETE", path, null)
        return code == 200
    }

    /**
     * Long-lived SSE reader with auto-reconnect + backoff.
     * onEvent("put"|"patch", path, dataJson) / onLost(reason) / onReady()
     * fires after subscription is live and BEFORE any event (gap-fill hook).
     */
    fun stream(path: String,
               onEvent: (String, String, JSONObject?) -> Unit,
               onLost: (String) -> Unit,
               onReady: () -> Unit,
               stop: () -> Boolean) {
        var backoff = 500L
        while (!stop()) {
            var conn: HttpURLConnection? = null
            try {
                // resolve redirects first (plain GET, no Accept header)
                var url = url(path)
                repeat(5) {
                    val c = open("GET", url)
                    c.connectTimeout = 10_000
                    val code = c.responseCode
                    if (code in 301..308) {
                        val loc = c.getHeaderField("Location")!!
                        url = URI(url).resolve(loc).toString()
                        c.disconnect()
                    } else {
                        c.disconnect()
                        return@repeat
                    }
                }
                conn = open("GET", url)
                conn.connectTimeout = 10_000
                conn.readTimeout = 0                     // SSE: no read timeout
                conn.setRequestProperty("Accept", "text/event-stream")
                val code = conn.responseCode
                if (code != 200) throw RuntimeException("SSE status $code")
                onReady()
                val reader = BufferedReader(
                    InputStreamReader(conn.inputStream, StandardCharsets.UTF_8))
                var event: String? = null
                var data: String? = null
                while (!stop()) {
                    val line = reader.readLine() ?: throw RuntimeException("SSE EOF")
                    when {
                        line.startsWith("event:") -> event = line.substring(6).trim()
                        line.startsWith("data:") -> data = line.substring(5).trim()
                        line.isEmpty() && event != null -> {
                            if (event == "put" || event == "patch") {
                                val payload = if (data.isNullOrEmpty() || data == "null")
                                    JSONObject() else JSONObject(data)
                                onEvent(event, payload.optString("path", "/"), payload.optJSONObject("data"))
                            } else if (event == "auth_revoked") {
                                throw RuntimeException("auth_revoked")
                            } else if (event == "cancel") {
                                throw RuntimeException("cancel: permission/policy")
                            }
                            event = null; data = null
                        }
                    }
                }
                throw RuntimeException("stream ended")
            } catch (e: Exception) {
                if (stop()) return
                onLost("${e.javaClass.simpleName}: ${e.message}")
                Thread.sleep(backoff + (System.nanoTime() % 250_000_000) / 1_000_000)
                backoff = minOf(backoff * 2, 10_000L)
            } finally {
                conn?.disconnect()
            }
        }
    }
}

/** Parse the SSE "path" value ("/z/<sid>/u/<seq>") into its last segment. */
fun lastSegment(p: String): String {
    val seg = p.trim('/').substringAfterLast('/')
    return URLDecoder.decode(seg, "UTF-8")
}
