package ir.zpoint.client

import android.app.Application
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.net.VpnService
import android.util.Base64
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import ir.zpoint.client.engine.GasCarrier
import ir.zpoint.client.engine.GasClientNode
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.atomic.AtomicLong

/**
 * App state + engine bridge (MVVM).  The UI observes immutable snapshots;
 * every engine mutation happens on Dispatchers.IO.
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 */
data class Profile(
    val name: String,
    val urls: List<String>,
    val token: String,
    val session: String,
    val key: String,
    val upPrefix: String,
    val downPrefix: String,
    val clientId: String,
    val socksPort: Int,
) {
    fun toJson(): String = JSONObject()
        .put("transport", "gas")
        .put("gas_urls", JSONArray(urls))
        .put("gas_token", token)
        .put("session", session)
        .put("key", key)
        .put("up_prefix", upPrefix)
        .put("down_prefix", downPrefix)
        .put("client_id", clientId)
        .put("kind", "client")
        .toString()

    companion object {
        fun fromJson(name: String, json: String): Profile {
            val c = JSONObject(json)
            val urls = mutableListOf<String>()
            val arr = c.optJSONArray("gas_urls") ?: JSONArray()
            for (i in 0 until arr.length()) urls.add(arr.getString(i))
            return Profile(
                name = name,
                urls = urls,
                token = c.optString("gas_token", ""),
                session = c.optString("session", ""),
                key = c.optString("key", ""),
                upPrefix = c.optString("up_prefix", ""),
                downPrefix = c.optString("down_prefix", ""),
                clientId = c.optString("client_id", "c1"),
                socksPort = c.optInt("socks_port", 1086),
            )
        }
    }
}

data class PoolHealth(val total: Int, val up: Int, val pings: Map<String, Long>)

data class UiState(
    val phase: Phase = Phase.DISCONNECTED,
    val profile: Profile? = null,
    val profiles: List<Profile> = emptyList(),
    val pool: PoolHealth? = null,
    val pingMs: Long? = null,
    val dlBps: Long = 0,
    val ulBps: Long = 0,
    val speedHistory: List<Pair<Long, Long>> = emptyList(),  // (dl, ul) samples
    val streams: Int = 0,
    val framesUp: Long = 0,
    val framesDown: Long = 0,
    val socksPort: Int = 1086,
    val vpnMode: Boolean = false,
    val logs: List<LogLine> = emptyList(),
    val error: String? = null,
    val vpnPrompt: Boolean = false,
)

enum class Phase { DISCONNECTED, CONNECTING, CONNECTED }

data class LogLine(val ts: Long, val tag: String, val text: String)

class ZpointViewModel(app: Application) : AndroidViewModel(app) {

    companion object {
        private const val PREFS = "zpoint_gui"
        private const val KEY_PROFILES = "profiles_json"
        private const val KEY_ACTIVE = "active_profile"
        private const val KEY_PORT = "socks_port"
        private const val SCHEME = "zpoint"
    }

    /** Holder for the VpnService consent intent until the activity
     *  launches it (activity result flows back via onVpnResult). */
    object pendingVpnIntent { @Volatile var intent: Intent? = null }

    private val ctx = app as Application

    private val _state = MutableStateFlow(UiState())
    val state: StateFlow<UiState> = _state


    init {
        _state.value = _state.value.copy(profiles = loadProfiles(),
                                         socksPort = prefs().getInt(KEY_PORT, 1086),
                                         vpnMode = prefs().getBoolean("vpn_mode", false))
        activeProfileName()?.let { n ->
            _state.value.profiles.firstOrNull { it.name == n }?.let {
                _state.value = _state.value.copy(profile = it)
            }
        }
        // v4.3: single phase collector for the whole ViewModel lifetime
        // (previously a new collector leaked on every connect toggle).
        viewModelScope.launch {
            EngineBus.phase.collect { ph ->
                _state.value = _state.value.copy(phase = ph)
                if (ph == Phase.CONNECTED) {
                    log("state", if (_state.value.vpnMode)
                        "connected — VPN mode (full device)"
                    else "connected — SOCKS5 127.0.0.1:${_state.value.socksPort}")
                    pingPool()
                }
            }
        }
        // single speed/streams sampler (previously leaked per-connect too)
        viewModelScope.launch {
            while (true) {
                delay(1000)
                if (_state.value.phase != Phase.CONNECTED) continue
                val sn = EngineBus.snapshot.value
                val hist = (_state.value.speedHistory +
                    (sn.dlBps to sn.ulBps)).takeLast(60)
                _state.value = _state.value.copy(
                    dlBps = sn.dlBps, ulBps = sn.ulBps,
                    speedHistory = hist, streams = sn.streams,
                    framesUp = sn.framesUp, framesDown = sn.framesDown)
            }
        }
    }

    // ---------------- profiles ----------------
    private fun prefs() = ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE)

    private fun loadProfiles(): List<Profile> {
        val raw = prefs().getString(KEY_PROFILES, "[]") ?: "[]"
        return try {
            val arr = JSONArray(raw)
            (0 until arr.length()).mapNotNull { i ->
                val o = arr.getJSONObject(i)
                Profile.fromJson(o.getString("name"), o.getString("json"))
            }
        } catch (_: Exception) { emptyList() }
    }

    private fun saveProfiles(profiles: List<Profile>) {
        val arr = JSONArray()
        profiles.forEach { arr.put(JSONObject().put("name", it.name)
            .put("json", it.toJson())) }
        prefs().edit().putString(KEY_PROFILES, arr.toString()).apply()
    }

    private fun activeProfileName(): String? =
        prefs().getString(KEY_ACTIVE, null)

    fun selectProfile(p: Profile) {
        prefs().edit().putString(KEY_ACTIVE, p.name).apply()
        _state.value = _state.value.copy(profile = p)
        log("profile", "active: ${p.name}")
    }

    fun saveProfile(p: Profile) {
        val list = loadProfiles().filterNot { it.name == p.name } + p
        saveProfiles(list)
        _state.value = _state.value.copy(profiles = list)
        if (_state.value.profile?.name == p.name || _state.value.profile == null)
            selectProfile(p)
    }

    fun deleteProfile(p: Profile) {
        val list = loadProfiles().filterNot { it.name == p.name }
        saveProfiles(list)
        val s = _state.value
        _state.value = s.copy(
            profiles = list,
            profile = s.profile?.takeIf { it.name != p.name })
    }

    fun setPort(port: Int) {
        prefs().edit().putInt(KEY_PORT, port).apply()
        _state.value = _state.value.copy(socksPort = port)
    }

    // ---------------- import (paste / deeplink / b64) ----------------
    /** Returns null on success, error text otherwise. */
    fun importPayload(raw: String, nameHint: String? = null): String? {
        val payload = if (raw.trim().startsWith("$SCHEME://")) {
            val q = Uri.parse(raw.trim())
            q.getQueryParameter("cfg") ?: return "deep link missing cfg"
        } else raw.trim()
        val json = try {
            // base64?
            if (!payload.startsWith("{")) String(
                Base64.decode(payload,
                              Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING))
            else payload
        } catch (_: IllegalArgumentException) {
            return "payload is neither JSON nor base64"
        }
        return try {
            val p = Profile.fromJson(
                nameHint ?: "gas-${System.currentTimeMillis() / 1000}", json)
            require(p.transportValid()) { "missing gas fields" }
            saveProfile(p)
            log("import", "profile '${p.name}' (${p.urls.size} nodes)")
            null
        } catch (e: Exception) {
            "invalid config: ${e.message}"
        }
    }

    private fun Profile.transportValid() =
        urls.isNotEmpty() && token.isNotEmpty() && session.isNotEmpty() &&
                key.isNotEmpty() && upPrefix.isNotEmpty() && downPrefix.isNotEmpty()

    // ---------------- connect lifecycle ----------------
    fun onVpnResult(granted: Boolean) {
        if (!granted) {
            _state.value = _state.value.copy(
                phase = Phase.DISCONNECTED, vpnPrompt = false,
                error = "VPN permission denied")
            return
        }
        _state.value = _state.value.copy(vpnPrompt = false)
        startEngine(vpn = true)
    }

    fun setVpnMode(on: Boolean) {
        prefs().edit().putBoolean("vpn_mode", on).apply()
        _state.value = _state.value.copy(vpnMode = on)
        val p = _state.value.socksPort
        log("mode", if (on) "VPN mode (full device)" else "Proxy mode (SOCKS5 :$p)")
    }

    fun toggleConnect(prepareVpn: () -> Unit) {
        when (_state.value.phase) {
            Phase.DISCONNECTED -> {
                val p = _state.value.profile ?: run {
                    _state.value = _state.value.copy(error = "import a config first")
                    return
                }
                _state.value = _state.value.copy(
                    phase = Phase.CONNECTING, error = null)
                // VPN mode needs VpnService consent; proxy mode does not.
                if (_state.value.vpnMode) {
                    val intent = VpnService.prepare(ctx)
                    if (intent != null) {
                        _state.value = _state.value.copy(vpnPrompt = true)
                        pendingVpnIntent.intent = intent
                        prepareVpn()
                        return
                    }
                }
                startEngine(vpn = _state.value.vpnMode)
            }
            else -> disconnect()
        }
    }

    private fun startEngine(vpn: Boolean) {
        val p = _state.value.profile ?: return
        val port = _state.value.socksPort
        if (vpn) {
            // VPN path: VpnTunnelService owns the engine AND the TUN bridge
            ctx.startForegroundService(
                Intent(ctx, VpnTunnelService::class.java)
                    .setAction(VpnTunnelService.ACTION_START)
                    .putExtra(VpnTunnelService.EXTRA_CONFIG, p.toJson())
                    .putExtra(VpnTunnelService.EXTRA_PORT, port))
            EngineBus.phase.value = Phase.CONNECTING
        } else {
            ctx.startForegroundService(EngineBus.newIntent(ctx, p.toJson(), port))
        }
        // v4.3 fix: the phase collector was previously RE-LAUNCHED on every
        // connect (leak: N collectors after N toggles, each capturing an old
        // vpn flag). Collect ONCE from init instead.
    }

    fun disconnect() {
        // stop BOTH paths — whichever is active shuts down cleanly
        try {
            ctx.startService(Intent(ctx, ir.zpoint.client.TunnelService::class.java)
                .setAction(ir.zpoint.client.TunnelService.ACTION_STOP))
        } catch (_: Exception) {}
        try {
            ctx.startService(Intent(ctx, ir.zpoint.client.VpnTunnelService::class.java)
                .setAction(ir.zpoint.client.VpnTunnelService.ACTION_STOP))
        } catch (_: Exception) {}
        _state.value = _state.value.copy(
            phase = Phase.DISCONNECTED, pingMs = null,
            pool = null, dlBps = 0, ulBps = 0,
            streams = 0, speedHistory = emptyList())
        log("state", "disconnected")
    }

    /** One-shot latency test of every pool script (UI "Ping All"). */
    fun pingPool() {
        val p = _state.value.profile ?: return
        viewModelScope.launch(Dispatchers.IO) {
            val results = java.util.concurrent.ConcurrentHashMap<String, Long>()
            val failures = java.util.concurrent.atomic.AtomicInteger(0)
            val jobs = p.urls.map { u ->
                launch {
                    try {
                        results[u] = GasCarrier(u, p.token, "gas").ping()
                    } catch (_: Exception) {
                        failures.incrementAndGet()
                    }
                }
            }
            jobs.forEach { it.join() }
            val ph = PoolHealth(p.urls.size,
                                p.urls.size - failures.get(), results)
            val ping = results.values.minOrNull()
            withContext(Dispatchers.Main) {
                _state.value = _state.value.copy(pool = ph, pingMs = ping)
            }
            log("health", "pool ${ph.up}/${ph.total} up" +
                    (ping?.let { " — best ${it}ms" } ?: ""))
        }
    }


    fun clearError() {
        _state.value = _state.value.copy(error = null)
    }

    private fun log(tag: String, text: String) {
        val line = LogLine(System.currentTimeMillis(), tag, text)
        _state.value = _state.value.copy(
            logs = (_state.value.logs + line).takeLast(500))
    }
}
