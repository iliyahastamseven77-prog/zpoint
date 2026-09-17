package ir.zpoint.client

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import ir.zpoint.client.engine.GasClientNode

/**
 * Foreground service hosting the Zpoint GAS engine (SOCKS5 on loopback).
 * Engine ownership lives in EngineBus so both the service and the UI
 * observe one source of truth; stop is deterministic (no zombies).
 *
 * HARDENING (v4.3 crash pass):
 *  - every engine call is wrapped; an engine failure can never take the
 *    process down (it surfaces in the UI error bar instead).
 *  - startForeground is ALWAYS called within 5s of startForegroundService
 *    (Android 8+ ANR/crash "Context.startForegroundService() did not then
 *    call Service.startForeground()" — the classic silent killer when
 *    EngineBus.start throws before the notify).
 *  - notification updates run on the main looper (NotificationManager is
 *    not guaranteed thread-safe from a raw coroutine thread).
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 */
class TunnelService : Service() {

    companion object {
        const val CHANNEL_ID = "zpoint_tunnel"
        const val NOTIF_ID = 0x7A
        const val ACTION_START = "ir.zpoint.START"
        const val ACTION_STOP = "ir.zpoint.STOP"
        const val EXTRA_CONFIG = "config_json"
        const val EXTRA_PORT = "socks_port"
    }

    private val mainHandler = Handler(Looper.getMainLooper())
    private val notifTick = object : Runnable {
        override fun run() {
            try {
                if (EngineBus.phase.value != Phase.DISCONNECTED) {
                    val nm = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
                    val p = EngineBus.phase.value
                    nm.notify(NOTIF_ID, notif(when (p) {
                        Phase.CONNECTED ->
                            "Connected · ${EngineBus.snapshot.value.streams} active streams"
                        Phase.CONNECTING -> "Connecting…"
                        else -> "Disconnected"
                    }))
                    mainHandler.postDelayed(this, 4000)
                }
            } catch (_: Exception) {}
        }
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_STOP -> {
                safeStop()
                return START_NOT_STICKY
            }
            else -> {
                val cfg = intent?.getStringExtra(EXTRA_CONFIG)
                val port = intent?.getIntExtra(EXTRA_PORT, 1086) ?: 1086
                // MUST be inside onStartCommand, synchronously — the OS gives
                // ~5s or the app crashes with ForegroundServiceDidNotStartInTime.
                startForeground(NOTIF_ID, notif("Connecting…"))
                mainHandler.postDelayed(notifTick, 4000)
                if (cfg != null) {
                    Thread({
                        try { EngineBus.start(cfg, port) }
                        catch (e: Exception) {
                            EngineBus.logs.append("error", "start failed: ${e.message}")
                        }
                    }, "zp-engine-start").start()
                }
            }
        }
        return START_STICKY
    }

    private fun safeStop() {
        try { EngineBus.stop() } catch (_: Exception) {}
        try {
            stopForeground(STOP_FOREGROUND_REMOVE)
        } catch (_: Exception) {}
        try { stopSelf() } catch (_: Exception) {}
    }

    override fun onDestroy() {
        mainHandler.removeCallbacks(notifTick)
        try { EngineBus.stop() } catch (_: Exception) {}
        super.onDestroy()
    }

    private fun notif(text: String): Notification {
        val nm = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
        if (Build.VERSION.SDK_INT >= 26) {
            nm.createNotificationChannel(
                NotificationChannel(CHANNEL_ID, "Zpoint Tunnel",
                                    NotificationManager.IMPORTANCE_LOW))
        }
        return Notification.Builder(this, CHANNEL_ID)
            .setContentTitle("Zpoint")
            .setContentText(text)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setOngoing(true)
            .build()
    }
}
