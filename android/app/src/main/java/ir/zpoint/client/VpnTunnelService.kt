package ir.zpoint.client

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Intent
import android.net.VpnService
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.ParcelFileDescriptor

/**
 * VPN mode v2 — VpnService owns the TUN; hev-socks5-tunnel (native,
 * production userspace TCP/IP stack) relays it into the engine's loopback
 * SOCKS5 listener.  Full TCP state machine + UDP/DNS relay, unlike the old
 * in-Kotlin packet synthesizer that could not carry real traffic.
 *
 * HARDENING (v4.3):
 *  - startForeground ALWAYS runs synchronously in onStartCommand (the
 *    ForegroundServiceDidNotStartInTime crash path is closed).
 *  - establish() failure → foreground notification with the reason, then a
 *    clean stop (no silent zombie service holding the app "open").
 *  - the TUN pfd is kept referenced until stop (no GC-finalizer fd close
 *    race that previously killed the tunnel mid-connection).
 *  - native stack stop is idempotent and guarded.
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 */
class VpnTunnelService : VpnService() {

    companion object {
        const val ACTION_START = "ir.zpoint.VPN_START"
        const val ACTION_STOP = "ir.zpoint.VPN_STOP"
        const val EXTRA_CONFIG = "config_json"
        const val EXTRA_PORT = "socks_port"
        private const val VPN_MTU = 8500      // hev default sweet spot
        private const val NOTIF_ID = 0x7B
    }

    private var tun: ParcelFileDescriptor? = null
    private val mainHandler = Handler(Looper.getMainLooper())

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_STOP -> {
                startForeground(NOTIF_ID, notif("Stopping…"))
                stopVpn()
                return START_NOT_STICKY
            }
            else -> {
                val cfg = intent?.getStringExtra(EXTRA_CONFIG)
                val port = intent?.getIntExtra(EXTRA_PORT, 1086) ?: 1086
                // synchronous foreground start (Android 8+ hard requirement)
                startForeground(NOTIF_ID, notif("Connecting (VPN)…"))
                if (cfg != null) startVpn(cfg, port)
            }
        }
        return START_STICKY
    }

    private fun startVpn(cfgJson: String, socksPort: Int) {
        // engine on loopback (same as proxy mode) — idempotent in EngineBus
        try { EngineBus.start(cfgJson, socksPort) }
        catch (e: Exception) {
            EngineBus.logs.append("error", "engine start failed: ${e.message}")
        }
        val builder = Builder()
            .setSession("Zpoint VPN")
            .setMtu(VPN_MTU)
            .addAddress("10.111.0.1", 30)
            .addRoute("0.0.0.0", 0)               // everything via TUN
            .addDnsServer("10.111.0.1")           // DNS through the tunnel (hev UDP relay)
        try { builder.addDisallowedApplication(packageName) }
        catch (_: Exception) {}
        val pfd = try { builder.establish() } catch (e: Exception) {
            EngineBus.logs.append("vpn", "establish() threw: ${e.message}")
            null
        }
        if (pfd == null) {
            EngineBus.logs.append("vpn", "establish() failed — VPN permission missing")
            notifAndStop("VPN permission missing — reconnect from the app")
            return
        }
        tun = pfd
        val ok = try {
            HevTunnel.start(this, pfd.fd, socksPort, VPN_MTU) { m ->
                EngineBus.logs.append("tun", m)
            }
        } catch (e: Throwable) {
            EngineBus.logs.append("vpn", "native tunnel failed: ${e.message}")
            false
        }
        if (!ok) { notifAndStop("VPN tunnel failed to start"); return }
        EngineBus.logs.append("vpn", "TUN up (mtu=$VPN_MTU) -> socks:$socksPort")
        updateNotif("VPN connected")
    }

    private fun updateNotif(text: String) {
        try {
            val nm = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
            nm.notify(NOTIF_ID, notif(text))
        } catch (_: Exception) {}
    }

    private fun notifAndStop(text: String) {
        updateNotif(text)
        mainHandler.postDelayed({ stopVpn() }, 1500)
    }

    private fun stopVpn() {
        try { HevTunnel.stop { m -> EngineBus.logs.append("tun", m) } }
        catch (_: Throwable) {}
        try { EngineBus.stop() } catch (_: Exception) {}
        try { tun?.close() } catch (_: Exception) {}
        tun = null
        try { stopForeground(STOP_FOREGROUND_REMOVE) } catch (_: Exception) {}
        try { stopSelf() } catch (_: Exception) {}
    }

    override fun onDestroy() {
        try { HevTunnel.stop { } } catch (_: Throwable) {}
        try { tun?.close() } catch (_: Exception) {}
        tun = null
        try { EngineBus.stop() } catch (_: Exception) {}
        super.onDestroy()
    }

    private fun notif(text: String): Notification {
        val nm = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
        if (Build.VERSION.SDK_INT >= 26) {
            nm.createNotificationChannel(
                NotificationChannel(TunnelService.CHANNEL_ID,
                    "Zpoint Tunnel", NotificationManager.IMPORTANCE_LOW))
        }
        return Notification.Builder(this, TunnelService.CHANNEL_ID)
            .setContentTitle("Zpoint")
            .setContentText(text)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setOngoing(true)
            .build()
    }
}
