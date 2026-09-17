package ir.zpoint.client.ui

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.LinearEasing
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.tween
import androidx.compose.animation.animateColorAsState
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.expandVertically
import androidx.compose.animation.shrinkVertically
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsPressedAsState
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.rounded.Add
import androidx.compose.material.icons.rounded.ArrowUpward
import androidx.compose.material.icons.rounded.ArrowDownward
import androidx.compose.material.icons.rounded.Bolt
import androidx.compose.material.icons.rounded.ChevronRight
import androidx.compose.material.icons.rounded.Hub
import androidx.compose.material.icons.rounded.NetworkCheck
import androidx.compose.material.icons.rounded.Power
import androidx.compose.material.icons.rounded.Settings
import androidx.compose.material.icons.rounded.Terminal
import androidx.compose.material.icons.rounded.VpnKey
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Switch
import androidx.compose.material3.SwitchDefaults
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.scale
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.drawscope.clipPath
import androidx.compose.ui.graphics.drawscope.rotate
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import ir.zpoint.client.Phase
import ir.zpoint.client.PoolHealth
import ir.zpoint.client.Profile
import ir.zpoint.client.Z
import ir.zpoint.client.ZpointViewModel

/**
 * Zpoint — Obsidian dashboard v2.
 *
 * Structure (top → bottom):
 *   ┌ status bar: brand + live dot · [ + ] [ ⚙ ]
 *   ├ profile switcher card (tap → profile sheet)
 *   ├ HERO connect ring (tactile: press-scale, breathing glow, sweep arc)
 *   ├ metrics grid: ping · nodes · ↑ · ↓
 *   ├ throughput graph (Bezier, dual series, gradient fill)
 *   └ mode row + diagnostics entry (→ logs sheet)
 *
 * 100% English. Property of the @ily_bio research channel (@iliyahsatam).
 */

private fun fmtSpeed(bps: Long): String = when {
    bps >= 1_048_576 -> String.format("%.1f", bps / 1048576.0) + " MB/s"
    bps >= 1024 -> String.format("%.0f", bps / 1024.0) + " KB/s"
    else -> "$bps B/s"
}

private fun fmtSpeedShort(bps: Long): String = when {
    bps >= 1_048_576 -> String.format("%.1f", bps / 1048576.0)
    bps >= 1024 -> String.format("%.0f", bps / 1024.0)
    else -> "$bps"
}

private fun unitOf(bps: Long): String = when {
    bps >= 1_048_576 -> "MB/s"
    bps >= 1024 -> "KB/s"
    else -> "B/s"
}

// =====================================================================
// Root
// =====================================================================

@Composable
fun ZpointApp(vm: ZpointViewModel) {
    val s by vm.state.collectAsState()
    var showProfiles by remember { mutableStateOf(false) }
    var showLogs by remember { mutableStateOf(false) }
    var showImport by remember { mutableStateOf(false) }
    var showSettings by remember { mutableStateOf(false) }

    val t = rememberInfiniteTransition(label = "amb")
    val sweep by t.animateFloat(
        0f, 360f,
        infiniteRepeatable(tween(2600, easing = LinearEasing),
                           RepeatMode.Restart), label = "sw")
    val breathe by t.animateFloat(
        0.90f, 1.06f,
        infiniteRepeatable(tween(2200, easing = LinearEasing),
                           RepeatMode.Reverse), label = "br")

    Box(Modifier.fillMaxSize().background(Z.CanvasBrush)) {
        // ambient corner glow reacting to state
        val ambient = when (s.phase) {
            Phase.CONNECTED -> Z.Emerald
            Phase.CONNECTING -> Z.Amber
            Phase.DISCONNECTED -> Z.Violet
        }
        Canvas(Modifier.fillMaxSize()) {
            drawCircle(Brush.radialGradient(
                listOf(ambient.copy(alpha = 0.09f), Color.Transparent),
                center = Offset(size.width * 0.85f, size.height * 0.08f),
                radius = size.width * 0.75f),
                radius = size.width * 0.75f,
                center = Offset(size.width * 0.85f, size.height * 0.08f))
            drawCircle(Brush.radialGradient(
                listOf(Z.Cyan.copy(alpha = 0.06f), Color.Transparent),
                center = Offset(size.width * 0.1f, size.height * 0.92f),
                radius = size.width * 0.7f),
                radius = size.width * 0.7f,
                center = Offset(size.width * 0.1f, size.height * 0.92f))
        }

        Column(Modifier.fillMaxSize().statusBarsPadding()
            .padding(horizontal = Z.Gutter)) {

            TopBar(live = s.phase == Phase.CONNECTED,
                   onAdd = { showImport = true },
                   onSettings = { showSettings = true })

            VGap(14)

            ProfileSwitcher(profile = s.profile, health = s.pool,
                            phase = s.phase,
                            onTap = { showProfiles = true })

            Spacer(Modifier.weight(1f))

            HeroRing(phase = s.phase, sweep = sweep, breathe = breathe,
                     pingMs = s.pingMs,
                     onClick = { vm.toggleConnect { } })

            Spacer(Modifier.weight(1f))

            MetricsGrid(pingMs = s.pingMs, pool = s.pool,
                        dlBps = s.dlBps, ulBps = s.ulBps,
                        streams = s.streams)

            VGap(12)

            ThroughputPanel(history = s.speedHistory,
                            dl = s.dlBps, ul = s.ulBps,
                            live = s.phase == Phase.CONNECTED)

            VGap(12)

            BottomRow(vpnMode = s.vpnMode,
                      onMode = { vm.setVpnMode(it) },
                      onLogs = { showLogs = true },
                      logCount = s.logs.size)

            VGap(10)
            Spacer(Modifier.navigationBarsPadding())
        }

        // ---- error toast strip ----
        AnimatedVisibility(
            visible = s.error != null,
            enter = fadeIn() + expandVertically(),
            exit = fadeOut() + shrinkVertically(),
            modifier = Modifier.align(Alignment.TopCenter)
                .statusBarsPadding()
                .padding(start = 20.dp, end = 20.dp, top = 62.dp)) {
            Row(Modifier.fillMaxWidth()
                .clip(RoundedCornerShape(Z.RPill))
                .background(Z.Danger.copy(alpha = 0.14f))
                .border(1.dp, Z.Danger.copy(alpha = 0.4f),
                        RoundedCornerShape(Z.RPill))
                .clickable { vm.clearError() }
                .padding(14.dp),
                verticalAlignment = Alignment.CenterVertically) {
                Box(Modifier.size(7.dp).clip(CircleShape).background(Z.Danger))
                Spacer(Modifier.width(10.dp))
                Text(s.error ?: "", color = Z.Fg,
                     style = MaterialTheme.typography.bodyMedium,
                     modifier = Modifier.weight(1f))
                Text("DISMISS", style = MaterialTheme.typography.labelSmall,
                     color = Z.Danger)
            }
        }

        // ---- sheets ----
        if (showProfiles) ProfileSheet(
            profiles = s.profiles, active = s.profile, health = s.pool,
            onDismiss = { showProfiles = false },
            onSelect = { vm.selectProfile(it); showProfiles = false },
            onImport = { showImport = true },
            onPing = { vm.pingPool() },
            onDelete = { vm.deleteProfile(it) })

        if (showImport) ImportSheet(
            onDismiss = { showImport = false },
            onImport = { raw, name ->
                val err = vm.importPayload(raw, name)
                if (err == null) { showImport = false; showProfiles = false }
                err
            })

        if (showLogs) LogsSheet(s.logs, onDismiss = { showLogs = false })

        if (showSettings) SettingsSheet(
            socksPort = s.socksPort,
            onPort = { vm.setPort(it) },
            onDismiss = { showSettings = false })

        if (s.vpnPrompt) {
            val act = androidx.compose.ui.platform.LocalContext.current
                as? ir.zpoint.client.MainActivity
            androidx.compose.runtime.LaunchedEffect(s.vpnPrompt) {
                act?.launchVpnConsentPublic()
            }
        }
    }
}

// =====================================================================
// Top bar
// =====================================================================

@Composable
private fun TopBar(live: Boolean, onAdd: () -> Unit, onSettings: () -> Unit) {
    Row(Modifier.fillMaxWidth().padding(top = 12.dp),
        verticalAlignment = Alignment.CenterVertically) {
        StatusDot(Z.Emerald, live)
        Spacer(Modifier.width(11.dp))
        Column {
            Text("ZPOINT", style = MaterialTheme.typography.titleLarge,
                 color = Z.Fg)
            Text(if (live) "TUNNEL ACTIVE" else "STANDBY",
                 style = MaterialTheme.typography.labelSmall,
                 color = if (live) Z.Emerald else Z.MutDim)
        }
        Spacer(Modifier.weight(1f))
        GlyphButton(Icons.Rounded.Add, "Import", onAdd)
        Spacer(Modifier.width(9.dp))
        GlyphButton(Icons.Rounded.Settings, "Settings", onSettings)
    }
}

@Composable
private fun GlyphButton(icon: androidx.compose.ui.graphics.vector.ImageVector,
                        desc: String, onClick: () -> Unit) {
    val src = remember { MutableInteractionSource() }
    val pressed by src.collectIsPressedAsState()
    val sc by animateFloatAsState(if (pressed) 0.9f else 1f,
                                  tween(110, easing = ZEase), label = "gb")
    Box(Modifier.scale(sc).size(42.dp).clip(CircleShape)
        .background(Z.glassBrush(0.10f, 0.04f))
        .border(1.dp, Z.Hairline, CircleShape)
        .clickable(interactionSource = src,
                   indication = androidx.compose.material.ripple.rememberRipple(
                       color = Z.Emerald),
                   onClick = onClick),
        contentAlignment = Alignment.Center) {
        Icon(icon, desc, tint = Z.FgSoft, modifier = Modifier.size(19.dp))
    }
}

// =====================================================================
// Profile switcher
// =====================================================================

@Composable
private fun ProfileSwitcher(profile: Profile?, health: PoolHealth?,
                            phase: Phase, onTap: () -> Unit) {
    GlassCard(Modifier.fillMaxWidth(),
              accent = if (phase == Phase.CONNECTED) Z.Emerald else null,
              onClick = onTap, contentPadding = 15.dp) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Box(Modifier.size(40.dp).clip(RoundedCornerShape(12.dp))
                .background(if (phase == Phase.CONNECTED)
                    Z.EmeraldWell else Z.GlassLo)
                .border(1.dp, if (phase == Phase.CONNECTED)
                    Z.Emerald.copy(alpha = 0.4f) else Z.Hairline,
                    RoundedCornerShape(12.dp)),
                contentAlignment = Alignment.Center) {
                Icon(Icons.Rounded.Hub, "profile",
                     tint = if (phase == Phase.CONNECTED) Z.Emerald else Z.Mut,
                     modifier = Modifier.size(20.dp))
            }
            Spacer(Modifier.width(13.dp))
            Column(Modifier.weight(1f)) {
                Overline("ACTIVE PROFILE")
                Spacer(Modifier.height(3.dp))
                Text(profile?.name ?: "No profile",
                     style = MaterialTheme.typography.titleMedium,
                     color = if (profile == null) Z.MutDim else Z.Fg,
                     maxLines = 1)
                Spacer(Modifier.height(2.dp))
                Text(
                    if (profile == null) "Tap to import a config"
                    else "${profile.urls.size} relay nodes" +
                         (health?.let { " · ${it.up}/${it.total} online" } ?: ""),
                    style = MaterialTheme.typography.bodySmall, maxLines = 1)
            }
            Icon(Icons.Rounded.ChevronRight, "open", tint = Z.MutDim,
                 modifier = Modifier.size(22.dp))
        }
    }
}

// =====================================================================
// Hero connect ring
// =====================================================================

@Composable
private fun HeroRing(phase: Phase, sweep: Float, breathe: Float,
                     pingMs: Long?, onClick: () -> Unit) {
    val accent by animateColorAsState(
        when (phase) {
            Phase.CONNECTED -> Z.Emerald
            Phase.CONNECTING -> Z.Amber
            Phase.DISCONNECTED -> Z.MutDim
        }, tween(450, easing = ZEase), label = "acc")

    val src = remember { MutableInteractionSource() }
    val pressed by src.collectIsPressedAsState()
    val press by animateFloatAsState(if (pressed) 0.94f else 1f,
                                     tween(150, easing = ZEase), label = "pr")
    val live = phase == Phase.CONNECTED
    val fill by animateFloatAsState(if (live) 1f else 0f,
                                    tween(700, easing = ZEase), label = "fl")

    Box(Modifier.fillMaxWidth().height(268.dp),
        contentAlignment = Alignment.Center) {

        Box(Modifier.size(230.dp).scale(press).clip(CircleShape)
            .clickable(interactionSource = src,
                       indication = androidx.compose.material.ripple.rememberRipple(
                           color = accent, bounded = false),
                       onClick = onClick),
            contentAlignment = Alignment.Center) {

            Canvas(Modifier.fillMaxSize()) {
                val c = center
                val r = size.minDimension / 2f - 26.dp.toPx()

                // ---- ambient glow field ----
                val glowR = r + 40.dp.toPx() * (if (live) breathe else 1f)
                drawCircle(Brush.radialGradient(
                    listOf(accent.copy(alpha = if (live) 0.20f else 0.07f),
                           Color.Transparent),
                    center = c, radius = glowR),
                    radius = glowR, center = c)

                // ---- track ring ----
                drawCircle(Z.Hairline, r + 13.dp.toPx(), c,
                           style = Stroke(2.dp.toPx()))

                // ---- progress/state ring (fills as we connect) ----
                if (fill > 0.01f) {
                    drawArc(
                        brush = Brush.sweepGradient(
                            listOf(Z.Emerald, Z.Cyan, Z.EmeraldSoft, Z.Emerald)),
                        startAngle = -90f, sweepAngle = 360f * fill,
                        useCenter = false,
                        topLeft = Offset(c.x - r - 13.dp.toPx(),
                                         c.y - r - 13.dp.toPx()),
                        size = androidx.compose.ui.geometry.Size(
                            (r + 13.dp.toPx()) * 2, (r + 13.dp.toPx()) * 2),
                        style = Stroke(3.dp.toPx(), cap = StrokeCap.Round))
                }

                // ---- CONNECTING: rotating comet arc ----
                if (phase == Phase.CONNECTING) {
                    rotate(sweep, c) {
                        drawArc(
                            brush = Brush.sweepGradient(
                                listOf(Color.Transparent,
                                       Z.Amber.copy(alpha = 0.15f),
                                       Z.Amber)),
                            startAngle = 0f, sweepAngle = 110f,
                            useCenter = false,
                            topLeft = Offset(c.x - r - 13.dp.toPx(),
                                             c.y - r - 13.dp.toPx()),
                            size = androidx.compose.ui.geometry.Size(
                                (r + 13.dp.toPx()) * 2, (r + 13.dp.toPx()) * 2),
                            style = Stroke(3.dp.toPx(), cap = StrokeCap.Round))
                    }
                }

                // ---- glass core ----
                drawCircle(Brush.verticalGradient(listOf(
                    Color.White.copy(alpha = 0.085f),
                    Color.White.copy(alpha = 0.025f))), r, c)
                drawCircle(accent.copy(alpha = 0.30f), r, c,
                           style = Stroke(1.dp.toPx()))
                // inner accent hairline
                drawCircle(accent.copy(alpha = 0.16f), r - 12.dp.toPx(), c,
                           style = Stroke(1.dp.toPx()))
            }

            Column(horizontalAlignment = Alignment.CenterHorizontally) {
                Icon(if (live) Icons.Rounded.Bolt else Icons.Rounded.Power,
                     "power", tint = accent, modifier = Modifier.size(34.dp))
                Spacer(Modifier.height(9.dp))
                Text(when (phase) {
                        Phase.DISCONNECTED -> "CONNECT"
                        Phase.CONNECTING -> "CONNECTING"
                        Phase.CONNECTED -> "CONNECTED"
                     },
                     style = MaterialTheme.typography.labelLarge,
                     color = if (live) Z.Fg else accent, fontSize = 14.sp)
                if (live) {
                    Spacer(Modifier.height(5.dp))
                    Text(pingMs?.let { "$it ms" } ?: "measuring…",
                         style = MaterialTheme.typography.bodySmall,
                         fontFamily = FontFamily.Monospace,
                         color = Z.Emerald)
                } else if (phase == Phase.DISCONNECTED) {
                    Spacer(Modifier.height(5.dp))
                    Text("tap to start", style = MaterialTheme.typography.bodySmall)
                }
            }
        }
    }
}

// =====================================================================
// Metrics grid
// =====================================================================

@Composable
private fun MetricsGrid(pingMs: Long?, pool: PoolHealth?,
                        dlBps: Long, ulBps: Long, streams: Int) {
    Column {
        Row(Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            MetricTile(Icons.Rounded.NetworkCheck, "LATENCY",
                       pingMs?.toString() ?: "—", "ms", Z.Cyan,
                       Modifier.weight(1f))
            MetricTile(Icons.Rounded.Hub, "NODES",
                       "${pool?.up ?: 0}", "/${pool?.total ?: 0}", Z.Violet,
                       Modifier.weight(1f))
        }
        VGap(10)
        Row(Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            MetricTile(Icons.Rounded.ArrowDownward, "DOWNLOAD",
                       fmtSpeedShort(dlBps), unitOf(dlBps), Z.Emerald,
                       Modifier.weight(1f))
            MetricTile(Icons.Rounded.ArrowUpward, "UPLOAD",
                       fmtSpeedShort(ulBps), unitOf(ulBps), Z.Cyan,
                       Modifier.weight(1f))
        }
    }
}

@Composable
private fun MetricTile(icon: androidx.compose.ui.graphics.vector.ImageVector,
                       label: String, value: String, unit: String,
                       accent: Color, modifier: Modifier = Modifier) {
    GlassCard(modifier, shape = RoundedCornerShape(Z.RPill),
              contentPadding = 13.dp) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Icon(icon, label, tint = accent.copy(alpha = 0.85f),
                 modifier = Modifier.size(14.dp))
            Spacer(Modifier.width(6.dp))
            Overline(label)
        }
        Spacer(Modifier.height(7.dp))
        Row(verticalAlignment = Alignment.Bottom) {
            Text(value, color = Z.Fg, fontSize = 21.sp,
                 fontWeight = FontWeight.Bold,
                 fontFamily = FontFamily.Monospace)
            Spacer(Modifier.width(3.dp))
            Text(unit, color = Z.MutDim, fontSize = 11.sp,
                 fontFamily = FontFamily.Monospace,
                 modifier = Modifier.padding(bottom = 2.dp))
        }
    }
}

// =====================================================================
// Throughput panel — dual Bezier series with gradient fills
// =====================================================================

@Composable
private fun ThroughputPanel(history: List<Pair<Long, Long>>,
                            dl: Long, ul: Long, live: Boolean) {
    GlassCard(Modifier.fillMaxWidth(), contentPadding = 14.dp) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Overline("THROUGHPUT")
            Spacer(Modifier.weight(1f))
            LegendDot(Z.Emerald, "DL")
            Spacer(Modifier.width(12.dp))
            LegendDot(Z.Cyan, "UL")
        }
        Spacer(Modifier.height(10.dp))
        Box(Modifier.fillMaxWidth().height(78.dp)) {
            if (history.size >= 2) {
                BezierGraph(history, Modifier.fillMaxSize())
            } else {
                Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                    Text(if (live) "measuring…" else "no traffic yet",
                         style = MaterialTheme.typography.bodySmall)
                }
            }
        }
    }
}

@Composable
private fun LegendDot(c: Color, label: String) {
    Row(verticalAlignment = Alignment.CenterVertically) {
        Box(Modifier.size(6.dp).clip(CircleShape).background(c))
        Spacer(Modifier.width(5.dp))
        Text(label, style = MaterialTheme.typography.labelSmall, color = c)
    }
}

/** Smooth Catmull-Rom-ish cubic series with gradient fill + peak marker. */
@Composable
fun BezierGraph(history: List<Pair<Long, Long>>,
                modifier: Modifier = Modifier) {
    val maxV = maxOf(4096L,
        history.maxOfOrNull { maxOf(it.first, it.second) } ?: 0L).toFloat()
    Canvas(modifier) {
        val h = size.height
        val w = size.width

        // baseline grid (3 faint rules)
        for (i in 1..3) {
            val y = h * i / 4f
            drawLine(Z.Hairline.copy(alpha = 0.5f),
                     Offset(0f, y), Offset(w, y), 1f)
        }

        fun series(vals: List<Long>): Pair<Path, Path> {
            val n = vals.size
            val pts = vals.mapIndexed { i, v ->
                Offset(w * i / (n - 1).coerceAtLeast(1),
                       h - 3f - (h - 10f) * (v / maxV).coerceIn(0f, 1f))
            }
            val line = Path().apply {
                if (pts.isNotEmpty()) moveTo(pts[0].x, pts[0].y)
                for (i in 1 until pts.size) {
                    val p0 = pts[i - 1]; val p1 = pts[i]
                    val mx = (p0.x + p1.x) / 2f
                    cubicTo(mx, p0.y, mx, p1.y, p1.x, p1.y)
                }
            }
            val fill = Path().apply {
                addPath(line)
                lineTo(pts.last().x, h); lineTo(pts.first().x, h); close()
            }
            return line to fill
        }

        // upload behind
        val (lu, fu) = series(history.map { it.second })
        clipPath(fu) {
            drawRect(Brush.verticalGradient(listOf(
                Z.Cyan.copy(alpha = 0.26f), Color.Transparent)))
        }
        drawPath(lu, Z.Cyan.copy(alpha = 0.9f),
                 style = Stroke(1.8.dp.toPx(), cap = StrokeCap.Round))

        // download front
        val (ld, fd) = series(history.map { it.first })
        clipPath(fd) {
            drawRect(Brush.verticalGradient(listOf(
                Z.Emerald.copy(alpha = 0.30f), Color.Transparent)))
        }
        drawPath(ld, Z.Emerald, style = Stroke(2.2.dp.toPx(),
                                               cap = StrokeCap.Round))

        // live head marker on the download series
        val last = history.last().first
        val hy = h - 3f - (h - 10f) * (last / maxV).coerceIn(0f, 1f)
        drawCircle(Z.Emerald.copy(alpha = 0.25f), 6.dp.toPx(), Offset(w, hy))
        drawCircle(Z.Emerald, 2.6.dp.toPx(), Offset(w, hy))
    }
}

// =====================================================================
// Bottom row: mode toggle + diagnostics
// =====================================================================

@Composable
private fun BottomRow(vpnMode: Boolean, onMode: (Boolean) -> Unit,
                      onLogs: () -> Unit, logCount: Int) {
    Row(Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(10.dp)) {

        GlassCard(Modifier.weight(1.35f), shape = RoundedCornerShape(Z.RPill),
                  accent = if (vpnMode) Z.Emerald else null,
                  contentPadding = 13.dp) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Icon(Icons.Rounded.VpnKey, "mode",
                     tint = if (vpnMode) Z.Emerald else Z.Mut,
                     modifier = Modifier.size(15.dp))
                Spacer(Modifier.width(7.dp))
                Column(Modifier.weight(1f)) {
                    Overline(if (vpnMode) "VPN MODE" else "PROXY MODE",
                             color = if (vpnMode) Z.Emerald else Z.Mut)
                    Spacer(Modifier.height(2.dp))
                    Text(if (vpnMode) "Whole device" else "SOCKS5 loopback",
                         style = MaterialTheme.typography.bodySmall,
                         maxLines = 1)
                }
                Switch(checked = vpnMode, onCheckedChange = onMode,
                       colors = SwitchDefaults.colors(
                           checkedThumbColor = Z.OnAcc,
                           checkedTrackColor = Z.Emerald,
                           checkedBorderColor = Z.Emerald,
                           uncheckedThumbColor = Z.Mut,
                           uncheckedTrackColor = Z.GlassLo,
                           uncheckedBorderColor = Z.Hairline),
                       modifier = Modifier.scale(0.82f))
            }
        }

        GlassCard(Modifier.weight(0.85f), shape = RoundedCornerShape(Z.RPill),
                  onClick = onLogs, contentPadding = 13.dp) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Icon(Icons.Rounded.Terminal, "logs", tint = Z.Mut,
                     modifier = Modifier.size(15.dp))
                Spacer(Modifier.width(7.dp))
                Column(Modifier.weight(1f)) {
                    Overline("DIAGNOSTICS")
                    Spacer(Modifier.height(2.dp))
                    Text("$logCount events",
                         style = MaterialTheme.typography.bodySmall,
                         maxLines = 1)
                }
                Icon(Icons.Rounded.ChevronRight, "open", tint = Z.MutDim,
                     modifier = Modifier.size(18.dp))
            }
        }
    }
}
