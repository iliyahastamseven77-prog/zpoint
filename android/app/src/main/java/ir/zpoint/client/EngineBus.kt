package ir.zpoint.client

import android.content.Context
import android.content.Intent
import ir.zpoint.client.engine.GasClientNode
import java.util.concurrent.atomic.AtomicLong

/**
 * Process-wide engine singleton: ONE GasClientNode instance shared by the
 * foreground service and the Compose UI.  Eliminates double-start races
 * and guarantees the stop path runs exactly once.
 *
 * HARDENING (v4.3): plain daemon threads instead of a shared CoroutineScope
 * (the old scope was cancelled in TunnelService.onDestroy while a VPN-mode
 * metrics loop could still be alive — a silent process-state killer);
 * null-safe stats access; start() is idempotent.
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 */
object EngineBus {

    data class Snap(val streams: Int = 0, val framesUp: Long = 0,
                    val framesDown: Long = 0, val dlBps: Long = 0,
                    val ulBps: Long = 0)

    @Volatile var phase: kotlinx.coroutines.flow.MutableStateFlow<Phase> =
        kotlinx.coroutines.flow.MutableStateFlow(Phase.DISCONNECTED)
        private set
    @Volatile var snapshot = kotlinx.coroutines.flow.MutableStateFlow(Snap())
        private set
    var lastLog: kotlinx.coroutines.flow.MutableStateFlow<String> =
        kotlinx.coroutines.flow.MutableStateFlow("")
        private set

    private var node: GasClientNode? = null
    private var lastBytes = 0L to 0L
    private var lastTs = 0L

    val logs = object : AppendOnly {
        override fun append(tag: String, text: String) {
            lastLog.value = "$tag|$text"
        }
    }

    interface AppendOnly { fun append(tag: String, text: String) }

    fun start(configJson: String, port: Int) {
        if (phase.value != Phase.DISCONNECTED) return
        phase.value = Phase.CONNECTING

        // Phase 1 (synchronous, no network I/O): build the node and bind the
        // loopback SOCKS5 ServerSocket. This MUST stay synchronous because
        // VpnTunnelService.startVpn() starts the native hev tunnel pointing at
        // 127.0.0.1:<port> the moment start() returns -- if the listener were
        // not bound yet, every early connection would be refused.
        val n = try {
            val x = GasClientNode.fromConfig(configJson, port) { m ->
                logs.append("engine", m)
            }
            x.start()
            x
        } catch (e: Exception) {
            logs.append("error", "connect failed: ${e.message}")
            phase.value = Phase.DISCONNECTED
            return
        }
        node = n

        // Phase 2 (background): BUGFIX #22 -- the real end-to-end handshake.
        //
        // The old code flipped to CONNECTED the instant start() returned. But
        // start() only binds a local socket and spawns poll threads; it never
        // touches the network. So a wrong gas_token, a wrong session key, a
        // revoked deployment, an empty gas_urls pool, or simply no exit node
        // running ALL looked exactly like success. The UI went green and the
        // user was left staring at a "Connected" badge with a dead tunnel.
        // mode=ping does not help either: it only proves the Apps Script is
        // alive, and says nothing about the exit behind it.
        //
        // This must run OFF the caller's thread: VpnTunnelService.startVpn()
        // calls EngineBus.start() from onStartCommand, i.e. the MAIN thread,
        // and the handshake is a relay round trip that can take tens of
        // seconds. Blocking there would be a guaranteed ANR.
        Thread({
            try {
                val rtt = n.handshake(45_000)
                logs.append("state",
                    "handshake ok - exit ${n.exitInfo} - rtt ${rtt}ms")
                phase.value = Phase.CONNECTED
                metricsLoop()
            } catch (e: Exception) {
                logs.append("error", "no exit answered: ${e.message}")
                try { n.stopAll() } catch (_: Exception) {}
                if (node === n) node = null
                phase.value = Phase.DISCONNECTED
            }
        }, "zp-handshake").start()
    }

    fun stop() {
        val n = node
        node = null
        phase.value = Phase.DISCONNECTED
        Thread({
            try { n?.stopAll() } catch (_: Exception) {}
            logs.append("state", "engine stopped")
        }, "zp-engine-stop").start()
    }

    private fun metricsLoop() {
        Thread({
            while (phase.value == Phase.CONNECTED) {
                val n = node ?: break
                // BUGFIX #22: a tunnel that dies mid-session (exit killed,
                // token revoked, deployment replaced) used to keep showing a
                // permanently green "Connected". The heartbeat inside
                // GasClientNode clears handshakeOk after 75s of silence;
                // surface that here and try to recover.
                if (!n.handshakeOk) {
                    phase.value = Phase.CONNECTING
                    logs.append("state", "exit went silent - reconnecting")
                    Thread({
                        try {
                            n.handshake(120_000)
                            phase.value = Phase.CONNECTED
                            logs.append("state", "exit is back - handshake restored")
                            metricsLoop()
                        } catch (e: Exception) {
                            logs.append("error", "exit unreachable: ${e.message}")
                            phase.value = Phase.DISCONNECTED
                        }
                    }, "zp-hs-retry").start()
                    break
                }
                try {
                    val rx = n.stats["bytes_in"]?.get() ?: 0L
                    val tx = n.stats["bytes_out"]?.get() ?: 0L
                    val now = System.currentTimeMillis()
                    val dt = (now - lastTs).coerceAtLeast(1)
                    val dl = if (lastTs == 0L) 0L else (rx - lastBytes.first) * 1000 / dt
                    val ul = if (lastTs == 0L) 0L else (tx - lastBytes.second) * 1000 / dt
                    lastBytes = rx to tx; lastTs = now
                    snapshot.value = Snap(
                        streams = (n.stats["streams"]?.get() ?: 0L).toInt(),
                        framesUp = n.stats["frames_out"]?.get() ?: 0L,
                        framesDown = n.stats["frames_in"]?.get() ?: 0L,
                        dlBps = dl, ulBps = ul)
                } catch (_: Exception) {}
                try { Thread.sleep(1000) } catch (_: InterruptedException) { break }
            }
        }, "zp-metrics").start()
    }

    fun newIntent(ctx: Context, cfgJson: String, port: Int): Intent =
        Intent(ctx, TunnelService::class.java)
            .setAction(TunnelService.ACTION_START)
            .putExtra(TunnelService.EXTRA_CONFIG, cfgJson)
            .putExtra(TunnelService.EXTRA_PORT, port)
}
