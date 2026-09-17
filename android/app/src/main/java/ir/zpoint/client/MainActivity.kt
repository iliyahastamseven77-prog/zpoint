package ir.zpoint.client

import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.viewModels
import ir.zpoint.client.ui.ZpointApp

/**
 * Single-activity Compose host.  Also the zpoint:// deep-link target:
 *   zpoint://import?cfg=<base64url(json)>
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 */
class MainActivity : ComponentActivity() {

    private val vm: ZpointViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        handleDeepLink(intent?.data)
        // keyboard: edge-to-edge so imePadding() in sheets works correctly
        androidx.core.view.WindowCompat.setDecorFitsSystemWindows(window, false)
        setContent {
            ZpointTheme {
                ZpointApp(vm)
            }
        }
    }

    @Deprecated("Deprecated in Java")
    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        super.onActivityResult(requestCode, resultCode, data)
        if (requestCode == VPN_REQ) {
            val granted = resultCode == RESULT_OK
            if (granted) {
                // consent received → connect now (vpn path already armed)
                vm.onVpnResult(true)
            } else {
                vm.onVpnResult(false)
            }
        }
    }

    private fun launchVpnConsent() {
        val intent = ir.zpoint.client.ZpointViewModel.pendingVpnIntent.intent
        if (intent != null) {
            @Suppress("DEPRECATION")
            startActivityForResult(intent, VPN_REQ)
        }
    }

    /** Public hook for Compose (LaunchedEffect in ZpointApp). */
    fun launchVpnConsentPublic() = launchVpnConsent()

    override fun onNewIntent(intent: android.content.Intent?) {
        super.onNewIntent(intent)
        handleDeepLink(intent?.data)
    }

    private fun handleDeepLink(uri: android.net.Uri?) {
        if (uri == null || uri.scheme != "zpoint") return
        val cfg = uri.getQueryParameter("cfg") ?: return
        vm.importPayload(cfg)
    }

    companion object { const val VPN_REQ = 0x5A }
}
