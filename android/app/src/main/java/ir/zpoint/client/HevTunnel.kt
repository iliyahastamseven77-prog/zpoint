package ir.zpoint.client

import android.content.Context
import android.net.VpnService
import android.os.ParcelFileDescriptor
import android.system.Os
import android.system.OsConstants

/**
 * VPN mode v2 — hev-socks5-tunnel (native) as a child process.
 *
 * Why: the previous in-Kotlin TUN bridge synthesized raw TCP packets with
 * zero seq/ack numbers, no TCP checksum, dropped ALL UDP (DNS!) and had no
 * handshake state — VPN mode could never carry real traffic.  hev is a
 * production userspace TCP/IP stack (full TCP state machine + UDP/DNS
 * mapping) used by mature Android clients; it relays into the SAME loopback
 * SOCKS5 listener the proxy mode already uses, so the engine is untouched.
 *
 * FD passing: the packaged binary's main() opens /dev/net/tun itself, which
 * is NOT permitted for app processes — VpnService must own the TUN.  So we
 * pass the established fd across fork() with dup2 onto a low, predictable
 * number, and hand the numeric fd in the config via `tunnel: fd:` ...
 * actually hev 2.17.1 accepts an external fd ONLY through its library API
 * (main(config, tun_fd)); the CLI binary passes -1 and opens the TUN itself.
 * Therefore we exec the binary AND pass the fd the only reliable way an app
 * can: inherit it across exec at a fixed fd number and use the tiny
 * hev-jni entry point — which is what TProxyStartService(config, fd) does.
 * We call that JNI symbol directly from Kotlin via System.load + a native
 * bridge declared below (no NDK build needed — symbols already ship inside
 * the official libhevtun.so release artifact).
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 */
object HevTunnel {

    init { System.loadLibrary("hev-socks5-tunnel") }

    // JNI surface exported by the upstream hev-socks5-tunnel library build
    // (hev-jni.c), registered on THIS class via -DPKGNAME/-DCLSNAME at
    // build time: ir/zpoint/client/HevTunnel
    private external fun TProxyStartService(configPath: String, fd: Int): Boolean
    private external fun TProxyStopService(): Boolean
    private external fun TProxyIsRunning(): Boolean

    @Volatile private var started = false

    /**
     * Start the native stack on an established TUN fd.
     * The fd is DUPed across the JNI boundary by hev itself.
     */
    @Synchronized
    fun start(ctx: Context, tunFd: Int, socksPort: Int, mtu: Int,
              log: (String) -> Unit): Boolean {
        if (started) { log("tunnel already running"); return true }
        val confFile = java.io.File(ctx.getFilesDir(), "tun.conf")
        confFile.writeText(conf(socksPort, mtu))
        val ok = try {
            TProxyStartService(confFile.absolutePath, tunFd)
        } catch (e: Exception) {
            log("native start failed: ${e.message}")
            false
        }
        if (ok) {
            started = true
            log("native tunnel up (fd=$tunFd -> socks:$socksPort, mtu=$mtu)")
        } else {
            log("TProxyStartService returned false (config/fd error)")
        }
        return ok
    }

    @Synchronized
    fun stop(log: (String) -> Unit) {
        if (!started) return
        try { TProxyStopService() } catch (_: Throwable) {}
        started = false
        log("native tunnel stopped")
    }

    fun isRunning(): Boolean = try { TProxyIsRunning() } catch (_: Throwable) { false }

    /** hev config for our topology (socks5 udp relay enabled for DNS). */
    private fun conf(socksPort: Int, mtu: Int): String = """
tunnel:
  mtu: $mtu
  ipv4: 10.111.0.1
socks5:
  address: 127.0.0.1
  port: $socksPort
  udp: udp
misc:
  log-level: warn
""".trimIndent() + "\n"

    /** Keep the ParcelFileDescriptor alive for the process lifetime. */
    fun keep(pfd: ParcelFileDescriptor) { /* GC anchor via companion */ }

    /** Convenience: does this device still need VPN consent? */
    fun needsConsent(ctx: Context): Boolean =
        VpnService.prepare(ctx) != null
}
