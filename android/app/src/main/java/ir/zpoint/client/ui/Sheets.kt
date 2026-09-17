package ir.zpoint.client.ui

import androidx.compose.animation.core.tween
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.imePadding
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.rounded.Bolt
import androidx.compose.material.icons.rounded.Close
import androidx.compose.material.icons.rounded.ContentCopy
import androidx.compose.material.icons.rounded.DeleteOutline
import androidx.compose.material.icons.rounded.Hub
import androidx.compose.material.icons.rounded.NetworkCheck
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.OutlinedTextFieldDefaults
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import ir.zpoint.client.LogLine
import ir.zpoint.client.PoolHealth
import ir.zpoint.client.Profile
import ir.zpoint.client.Z

/**
 * Bottom sheets — Obsidian v2: frosted panels, luminous grab handle,
 * icon-led rows, tactile primary buttons.  100% English.
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 */

private val SheetShape = RoundedCornerShape(topStart = Z.RSheet,
                                            topEnd = Z.RSheet)
private val RowShape = RoundedCornerShape(Z.RPill)

@Composable
private fun SheetChrome(content: @Composable () -> Unit) {
    Box(Modifier.fillMaxSize().background(Color(0xCC05070B))) {
        Column(
            Modifier.align(Alignment.BottomCenter).fillMaxWidth()
                .clip(SheetShape)
                .background(Z.CanvasBrush)
                .border(1.dp, Z.Hairline, SheetShape)
                .padding(horizontal = 20.dp, vertical = 18.dp)
                .navigationBarsPadding()
                // keyboard: lift above the IME (pairs with adjustResize)
                .imePadding()
        ) { content() }
    }
}

@Composable
private fun SheetHeader(title: String, subtitle: String? = null,
                        onClose: () -> Unit,
                        trailing: (@Composable () -> Unit)? = null) {
    // grab handle
    Box(Modifier.fillMaxWidth(), contentAlignment = Alignment.Center) {
        Box(Modifier.width(42.dp).height(4.dp)
            .clip(RoundedCornerShape(2.dp))
            .background(Z.HairlineHi))
    }
    Spacer(Modifier.height(16.dp))
    Row(verticalAlignment = Alignment.CenterVertically) {
        Column(Modifier.weight(1f)) {
            Text(title, style = MaterialTheme.typography.headlineSmall,
                 color = Z.Fg)
            if (subtitle != null) {
                Spacer(Modifier.height(3.dp))
                Text(subtitle, style = MaterialTheme.typography.bodySmall)
            }
        }
        trailing?.invoke()
        Spacer(Modifier.width(8.dp))
        Box(Modifier.size(34.dp).clip(CircleShape)
            .background(Z.GlassLo).border(1.dp, Z.Hairline, CircleShape)
            .clickable { onClose() }, contentAlignment = Alignment.Center) {
            Icon(Icons.Rounded.Close, "close", tint = Z.Mut,
                 modifier = Modifier.size(16.dp))
        }
    }
    Spacer(Modifier.height(16.dp))
}

@Composable
private fun PrimaryButton(text: String, modifier: Modifier = Modifier,
                          onClick: () -> Unit) {
    Button(onClick = onClick, modifier = modifier.height(50.dp),
           shape = RoundedCornerShape(Z.RPill),
           colors = ButtonDefaults.buttonColors(
               containerColor = Z.Emerald, contentColor = Z.OnAcc)) {
        Text(text, style = MaterialTheme.typography.labelLarge)
    }
}

@Composable
private fun zField(value: String, onChange: (String) -> Unit,
                   placeholder: String, modifier: Modifier = Modifier) {
    OutlinedTextField(
        value = value, onValueChange = onChange,
        placeholder = { Text(placeholder,
                             style = MaterialTheme.typography.bodySmall) },
        modifier = modifier,
        shape = RowShape,
        textStyle = MaterialTheme.typography.bodyMedium.copy(color = Z.Fg),
        colors = OutlinedTextFieldDefaults.colors(
            focusedBorderColor = Z.Emerald.copy(alpha = 0.7f),
            unfocusedBorderColor = Z.Hairline,
            focusedContainerColor = Z.GlassLo,
            unfocusedContainerColor = Z.GlassLo,
            focusedTextColor = Z.Fg, unfocusedTextColor = Z.Fg,
            cursorColor = Z.Emerald))
}

// =====================================================================
// Profiles
// =====================================================================

@Composable
fun ProfileSheet(profiles: List<Profile>, active: Profile?,
                 health: PoolHealth?, onDismiss: () -> Unit,
                 onSelect: (Profile) -> Unit, onImport: () -> Unit,
                 onPing: () -> Unit, onDelete: (Profile) -> Unit) {
    SheetChrome {
        SheetHeader("Profiles", "${profiles.size} saved", onDismiss,
                    trailing = {
            TextButton(onClick = onPing) {
                Icon(Icons.Rounded.NetworkCheck, "ping", tint = Z.Emerald,
                     modifier = Modifier.size(15.dp))
                Spacer(Modifier.width(6.dp))
                Text("PING ALL", style = MaterialTheme.typography.labelMedium,
                     color = Z.Emerald)
            }
        })
        LazyColumn(Modifier.fillMaxWidth().heightIn(max = 320.dp),
                   verticalArrangement = Arrangement.spacedBy(9.dp)) {
            if (profiles.isEmpty()) {
                item {
                    GlassCard(Modifier.fillMaxWidth(), contentPadding = 22.dp) {
                        Text("No profiles yet",
                             style = MaterialTheme.typography.titleMedium,
                             color = Z.Fg)
                        Spacer(Modifier.height(5.dp))
                        Text("Import a GAS relay config to get started.",
                             style = MaterialTheme.typography.bodySmall)
                    }
                }
            }
            items(profiles.size) { i ->
                val p = profiles[i]
                val isActive = p.name == active?.name
                GlassCard(Modifier.fillMaxWidth(), shape = RowShape,
                          accent = if (isActive) Z.Emerald else null,
                          onClick = { onSelect(p) }, contentPadding = 13.dp) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Box(Modifier.size(34.dp).clip(RoundedCornerShape(10.dp))
                            .background(if (isActive) Z.EmeraldWell else Z.GlassLo)
                            .border(1.dp, if (isActive)
                                Z.Emerald.copy(alpha = 0.4f) else Z.Hairline,
                                RoundedCornerShape(10.dp)),
                            contentAlignment = Alignment.Center) {
                            Icon(if (isActive) Icons.Rounded.Bolt
                                 else Icons.Rounded.Hub, "p",
                                 tint = if (isActive) Z.Emerald else Z.Mut,
                                 modifier = Modifier.size(17.dp))
                        }
                        Spacer(Modifier.width(11.dp))
                        Column(Modifier.weight(1f)) {
                            Text(p.name,
                                 color = if (isActive) Z.Emerald else Z.Fg,
                                 style = MaterialTheme.typography.titleMedium,
                                 maxLines = 1)
                            Spacer(Modifier.height(2.dp))
                            Text("${p.urls.size} nodes · ${p.session.take(10)}…",
                                 style = MaterialTheme.typography.bodySmall,
                                 maxLines = 1)
                        }
                        health?.pings?.get(p.urls.firstOrNull())?.let {
                            Text("$it ms", color = Z.Mut, fontSize = 11.sp,
                                 fontFamily = FontFamily.Monospace)
                            Spacer(Modifier.width(9.dp))
                        }
                        val clip = LocalClipboardManager.current
                        val ctx = androidx.compose.ui.platform.LocalContext.current
                        Icon(Icons.Rounded.ContentCopy, "share", tint = Z.Cyan,
                             modifier = Modifier.size(17.dp).clickable {
                                 val link = "zpoint://import?cfg=" +
                                     android.util.Base64.encodeToString(
                                         p.toJson().toByteArray(),
                                         android.util.Base64.URL_SAFE or
                                         android.util.Base64.NO_WRAP or
                                         android.util.Base64.NO_PADDING)
                                 clip.setText(AnnotatedString(link))
                                 android.widget.Toast.makeText(ctx,
                                     "zpoint:// link copied",
                                     android.widget.Toast.LENGTH_SHORT).show()
                             })
                        Spacer(Modifier.width(12.dp))
                        Icon(Icons.Rounded.DeleteOutline, "delete",
                             tint = Z.MutDim,
                             modifier = Modifier.size(18.dp)
                                 .clickable { onDelete(p) })
                    }
                }
            }
        }
        Spacer(Modifier.height(16.dp))
        PrimaryButton("IMPORT NEW CONFIG", Modifier.fillMaxWidth()) {
            onDismiss(); onImport()
        }
    }
}

// =====================================================================
// Import
// =====================================================================

@Composable
fun ImportSheet(onDismiss: () -> Unit,
                onImport: (raw: String, name: String?) -> String?) {
    var text by remember { mutableStateOf("") }
    var name by remember { mutableStateOf("") }
    var err by remember { mutableStateOf<String?>(null) }
    SheetChrome {
        SheetHeader("Import config",
                    "Paste JSON, base64, or a zpoint:// link", onDismiss)
        zField(text, { text = it; err = null },
               "zpoint://… or {\"transport\":\"gas\",…}",
               Modifier.fillMaxWidth().height(132.dp))
        Spacer(Modifier.height(10.dp))
        zField(name, { name = it }, "Profile name (optional)",
               Modifier.fillMaxWidth())
        if (err != null) {
            Spacer(Modifier.height(10.dp))
            Row(Modifier.fillMaxWidth().clip(RowShape)
                .background(Z.Danger.copy(alpha = 0.12f))
                .border(1.dp, Z.Danger.copy(alpha = 0.35f), RowShape)
                .padding(12.dp),
                verticalAlignment = Alignment.CenterVertically) {
                Box(Modifier.size(6.dp).clip(CircleShape).background(Z.Danger))
                Spacer(Modifier.width(9.dp))
                Text(err!!, color = Z.Fg,
                     style = MaterialTheme.typography.bodySmall)
            }
        }
        Spacer(Modifier.height(16.dp))
        Row(Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            TextButton(onClick = onDismiss, modifier = Modifier.weight(1f)
                .height(50.dp)) {
                Text("CANCEL", style = MaterialTheme.typography.labelMedium,
                     color = Z.Mut)
            }
            PrimaryButton("SAVE PROFILE", Modifier.weight(1.4f)) {
                err = onImport(text, name.ifBlank { null })
            }
        }
    }
}

// =====================================================================
// Settings
// =====================================================================

@Composable
fun SettingsSheet(socksPort: Int, onPort: (Int) -> Unit,
                  onDismiss: () -> Unit) {
    var port by remember { mutableStateOf(socksPort.toString()) }
    var saved by remember { mutableStateOf(false) }
    SheetChrome {
        SheetHeader("Settings", "Engine and routing", onDismiss)
        Overline("LOCAL SOCKS PORT")
        Spacer(Modifier.height(9.dp))
        zField(port, { v -> port = v.filter { it.isDigit() }.take(5); saved = false },
               "1086", Modifier.fillMaxWidth())
        Spacer(Modifier.height(9.dp))
        Text("The engine exposes a SOCKS5 proxy on 127.0.0.1 at this port. " +
             "Applies on the next connect. VPN mode routes the whole device " +
             "through the same engine.",
             style = MaterialTheme.typography.bodySmall)
        Spacer(Modifier.height(16.dp))
        GlassCard(Modifier.fillMaxWidth(), shape = RowShape,
                  contentPadding = 13.dp) {
            Overline("TRANSPORT")
            Spacer(Modifier.height(4.dp))
            Text("Google Apps Script relay pool · AES-256-GCM end-to-end",
                 style = MaterialTheme.typography.bodySmall)
        }
        Spacer(Modifier.height(16.dp))
        PrimaryButton(if (saved) "SAVED ✓" else "SAVE",
                      Modifier.fillMaxWidth()) {
            port.toIntOrNull()?.let { p ->
                if (p in 1024..65535) { onPort(p); saved = true }
            }
        }
    }
}

// =====================================================================
// Diagnostics / live logs
// =====================================================================

@Composable
fun LogsSheet(logs: List<LogLine>, onDismiss: () -> Unit) {
    SheetChrome {
        SheetHeader("Diagnostics", "${logs.size} events · newest last",
                    onDismiss)
        val lv = rememberLazyListState()
        LaunchedEffect(logs.size) {
            if (logs.isNotEmpty()) lv.animateScrollToItem(logs.size - 1)
        }
        LazyColumn(Modifier.fillMaxWidth().heightIn(max = 400.dp)
            .clip(RowShape).background(Z.GlassLo)
            .border(1.dp, Z.Hairline, RowShape)
            .padding(12.dp), state = lv) {
            if (logs.isEmpty()) {
                item {
                    Text("Waiting for engine activity…",
                         color = Z.MutDim, fontSize = 11.sp,
                         fontFamily = FontFamily.Monospace)
                }
            }
            items(logs.size) { i ->
                val l = logs[i]
                val color = when (l.tag) {
                    "state", "health", "import", "profile" -> Z.Emerald
                    "error" -> Z.Danger
                    "engine" -> Z.Amber
                    "tun", "vpn" -> Z.Violet
                    else -> Z.Mut
                }
                Row(Modifier.padding(vertical = 1.dp)) {
                    Text(java.text.SimpleDateFormat("HH:mm:ss")
                        .format(java.util.Date(l.ts)),
                         color = Z.MutDim, fontSize = 10.5.sp,
                         fontFamily = FontFamily.Monospace)
                    Spacer(Modifier.width(7.dp))
                    Text(l.tag.uppercase().take(6).padEnd(6),
                         color = color, fontSize = 10.5.sp,
                         fontWeight = FontWeight.Bold,
                         fontFamily = FontFamily.Monospace)
                    Spacer(Modifier.width(7.dp))
                    Text(l.text, color = Z.FgSoft, fontSize = 10.5.sp,
                         fontFamily = FontFamily.Monospace)
                }
            }
        }
        Spacer(Modifier.height(12.dp))
        Row(verticalAlignment = Alignment.CenterVertically) {
            Box(Modifier.size(6.dp).clip(CircleShape)
                .background(Z.Emerald.copy(alpha = 0.6f)))
            Spacer(Modifier.width(8.dp))
            Text("Sanitized — no keys or payloads are ever logged.",
                 style = MaterialTheme.typography.bodySmall)
        }
    }
}
