package ir.zpoint.client.ui

import androidx.compose.animation.core.Animatable
import androidx.compose.animation.core.CubicBezierEasing
import androidx.compose.animation.core.FastOutSlowInEasing
import androidx.compose.animation.core.LinearEasing
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.tween
import androidx.compose.animation.core.animateFloat
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsPressedAsState
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxScope
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.scale
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Shape
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import ir.zpoint.client.Z

/**
 * Obsidian design-system primitives: frosted glass, glow fields, tactile
 * press feedback, animated counters.  Everything here is pure Compose
 * (no external deps) and runs at 120Hz on mid-range hardware.
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 */

/** Signature easing: fast-out, gentle-in — the "butter" curve. */
val ZEase = CubicBezierEasing(0.22f, 1f, 0.36f, 1f)

/** Frosted-glass container: translucent white wash + luminous hairline. */
@Composable
fun GlassCard(
    modifier: Modifier = Modifier,
    shape: Shape = RoundedCornerShape(Z.RCard),
    accent: Color? = null,
    onClick: (() -> Unit)? = null,
    contentPadding: Dp = 16.dp,
    content: @Composable ColumnScope.() -> Unit,
) {
    val interaction = remember { MutableInteractionSource() }
    val pressed by interaction.collectIsPressedAsState()
    val scale by animateFloatAsState(
        if (pressed) 0.985f else 1f, tween(120, easing = ZEase), label = "sc")
    // ripple tinted to the card's accent (LocalIndication would be grey)
    val indication = androidx.compose.material.ripple.rememberRipple(
        color = accent ?: Z.Emerald)
    var m = modifier.scale(scale).clip(shape)
        .background(Z.glassBrush())
        .border(1.dp, accent?.copy(alpha = 0.45f) ?: Z.Hairline, shape)
    if (onClick != null) {
        m = m.then(Modifier.clickable(
            interactionSource = interaction,
            indication = indication,
            onClick = onClick))
    }
    Column(m.padding(contentPadding)) { content() }
}

/** Small overline label (wide tracking, muted). */
@Composable
fun Overline(text: String, modifier: Modifier = Modifier,
             color: Color = Z.Mut) {
    Text(text, style = MaterialTheme.typography.labelSmall,
         color = color, modifier = modifier)
}

/** Soft radial glow behind a composable (used by the hero + status dots). */
@Composable
fun GlowField(color: Color, radius: Dp, modifier: Modifier = Modifier,
              alpha: Float = 1f) {
    Canvas(modifier.size(radius * 2)) {
        drawCircle(Brush.radialGradient(
            listOf(color.copy(alpha = 0.35f * alpha),
                   color.copy(alpha = 0.10f * alpha),
                   Color.Transparent),
            center = center, radius = size.minDimension / 2f))
    }
}

/** Breathing status dot: pulses only when live. */
@Composable
fun StatusDot(color: Color, live: Boolean, size: Dp = 9.dp) {
    val t = rememberInfiniteTransition(label = "dot")
    val a by t.animateFloat(
        0.45f, 1f,
        infiniteRepeatable(tween(1300, easing = LinearEasing),
                           RepeatMode.Reverse), label = "a")
    Box(contentAlignment = Alignment.Center) {
        if (live) {
            Canvas(Modifier.size(size * 3)) {
                drawCircle(color.copy(alpha = 0.16f * a),
                           size.toPx() * 1.35f, center)
            }
        }
        Box(Modifier.size(size).clip(CircleShape)
            .background(if (live) color else Z.MutDim.copy(alpha = 0.55f)))
    }
}

/** Divider hairline. */
@Composable
fun Hairline(modifier: Modifier = Modifier) {
    Box(modifier.fillMaxWidth().height(1.dp).background(Z.Hairline))
}

/** Vertical spacer shorthand. */
@Composable
fun VGap(dp: Int) = Spacer(Modifier.height(dp.dp))
