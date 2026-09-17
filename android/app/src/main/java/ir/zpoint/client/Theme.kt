package ir.zpoint.client

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Typography
import androidx.compose.material3.darkColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp

/**
 * Zpoint design system — "Obsidian" v2.
 *
 * Deep obsidian canvas (#0D1117 → #0F172A gradient), frosted-glass
 * surfaces with luminous hairlines, neon emerald for live/active state,
 * sky-cyan for the uplink series.  Typography is a modern geometric
 * sans stack (Inter/Roboto via FontFamily.SansSerif on-device) with
 * deliberate tracking on labels and tabular monospace for numbers.
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 */
object Z {
    // ---- canvas ----
    val Bg = Color(0xFF0D1117)            // obsidian base
    val BgDeep = Color(0xFF0A0E14)        // vignette floor
    val BgTint = Color(0xFF0F172A)        // slate-navy top tint

    // ---- frosted glass surfaces ----
    val Glass = Color(0x14FFFFFF)         // 8% white — primary card fill
    val GlassHi = Color(0x1FFFFFFF)       // 12% — pressed / elevated
    val GlassLo = Color(0x0AFFFFFF)       // 4% — nested wells
    val Hairline = Color(0x1FFFFFFF)      // 12% white border
    val HairlineHi = Color(0x33FFFFFF)    // 20% — focused border

    // ---- type ----
    val Fg = Color(0xFFF8FAFC)            // primary
    val FgSoft = Color(0xFFCBD5E1)        // secondary
    val Mut = Color(0xFF8B98AB)           // tertiary / labels
    val MutDim = Color(0xFF5A6779)        // disabled

    // ---- accents ----
    val Emerald = Color(0xFF10E098)       // LIVE — neon emerald
    val EmeraldSoft = Color(0xFF34D399)
    val EmeraldGlow = Color(0x4010E098)   // glow field
    val EmeraldWell = Color(0xFF04231A)   // deep accent well
    val Cyan = Color(0xFF38BDF8)          // uplink series
    val CyanGlow = Color(0x3338BDF8)
    val Amber = Color(0xFFFBBF24)         // connecting
    val AmberGlow = Color(0x33FBBF24)
    val Violet = Color(0xFFA78BFA)        // secondary metric accent
    val Danger = Color(0xFFFB7185)        // errors only
    val OnAcc = Color(0xFF04231A)         // ink on emerald

    // ---- gradients ----
    val CanvasBrush = Brush.verticalGradient(
        0f to BgTint, 0.45f to Bg, 1f to BgDeep)

    val EmeraldSweep = Brush.horizontalGradient(
        listOf(Emerald, EmeraldSoft, Cyan))

    fun glassBrush(top: Float = 0.10f, bottom: Float = 0.03f) =
        Brush.verticalGradient(listOf(
            Color.White.copy(alpha = top),
            Color.White.copy(alpha = bottom)))

    fun glowBrush(c: Color) = Brush.radialGradient(
        listOf(c.copy(alpha = 0.45f), c.copy(alpha = 0.10f), Color.Transparent))

    // ---- geometry tokens ----
    val RCard = 20.dp
    val RPill = 14.dp
    val RSheet = 28.dp
    val Gutter = 20.dp
}

private val DarkScheme = darkColorScheme(
    primary = Z.Emerald,
    onPrimary = Z.OnAcc,
    primaryContainer = Z.EmeraldWell,
    onPrimaryContainer = Z.Emerald,
    secondary = Z.Cyan,
    onSecondary = Z.OnAcc,
    tertiary = Z.Violet,
    background = Z.Bg,
    onBackground = Z.Fg,
    surface = Z.Bg,
    onSurface = Z.Fg,
    surfaceVariant = Z.Glass,
    onSurfaceVariant = Z.Mut,
    surfaceContainer = Z.Bg,
    surfaceContainerHigh = Z.BgTint,
    outline = Z.Hairline,
    outlineVariant = Z.Hairline,
    error = Z.Danger,
    scrim = Color(0xCC05070B),
)

/** Modern geometric sans; on-device Inter/Roboto via SansSerif. */
private val Sans = FontFamily.SansSerif
private val Mono = FontFamily.Monospace

private val ZTypography = Typography(
    displaySmall = TextStyle(
        fontFamily = Sans, fontWeight = FontWeight.Bold,
        fontSize = 34.sp, letterSpacing = (-0.5).sp),
    headlineMedium = TextStyle(
        fontFamily = Sans, fontWeight = FontWeight.Bold,
        fontSize = 24.sp, letterSpacing = (-0.2).sp),
    headlineSmall = TextStyle(
        fontFamily = Sans, fontWeight = FontWeight.SemiBold,
        fontSize = 20.sp),
    titleLarge = TextStyle(
        fontFamily = Sans, fontWeight = FontWeight.Bold,
        fontSize = 19.sp, letterSpacing = 0.1.sp),
    titleMedium = TextStyle(
        fontFamily = Sans, fontWeight = FontWeight.SemiBold,
        fontSize = 15.sp, letterSpacing = 0.1.sp),
    bodyLarge = TextStyle(
        fontFamily = Sans, fontSize = 15.sp, color = Z.FgSoft),
    bodyMedium = TextStyle(
        fontFamily = Sans, fontSize = 13.5.sp, color = Z.FgSoft),
    bodySmall = TextStyle(
        fontFamily = Sans, fontSize = 12.sp, color = Z.Mut),
    // overline labels: wide tracking, muted
    labelSmall = TextStyle(
        fontFamily = Sans, fontSize = 10.sp,
        fontWeight = FontWeight.Bold, letterSpacing = 1.5.sp,
        color = Z.Mut),
    labelMedium = TextStyle(
        fontFamily = Sans, fontSize = 11.sp,
        fontWeight = FontWeight.SemiBold, letterSpacing = 0.8.sp),
    labelLarge = TextStyle(
        fontFamily = Sans, fontSize = 13.sp,
        fontWeight = FontWeight.Bold, letterSpacing = 0.9.sp),
)

/** Tabular numerals for metrics (no width jitter while counting). */
val NumStyle = TextStyle(fontFamily = Mono, fontWeight = FontWeight.Medium)

@Composable
fun ZpointTheme(content: @Composable () -> Unit) {
    @Suppress("UNUSED_EXPRESSION") isSystemInDarkTheme()   // always dark by design
    MaterialTheme(
        colorScheme = DarkScheme,
        typography = ZTypography,
        content = content
    )
}
