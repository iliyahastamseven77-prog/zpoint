package ir.zpoint.client.engine

import java.util.Base64
import java.util.concurrent.atomic.AtomicLong
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/**
 * AES-256-GCM end-to-end codec for transport frames.
 * Nonce: 4-byte random per-direction prefix + 8-byte big-endian transport seq.
 * Mirrors core/crypto.py (v2): seq range validation, tag length 128-bit.
 */
class FrameCrypto(key: ByteArray, val dirPrefix: ByteArray) {

    init {
        require(key.size == 32) { "session key must be 32 bytes" }
        require(dirPrefix.size == 4) { "dir prefix must be 4 bytes" }
    }

    private val keySpec = SecretKeySpec(key, "AES")
    private val lastSeq = AtomicLong(-1)

    fun seal(seq: Long, plaintext: ByteArray, aad: ByteArray): ByteArray {
        // BUGFIX (root cause #6): Kotlin's `1L shl 63` OVERFLOWS to
        // Long.MIN_VALUE (negative!), so `seq < (1L shl 63)` was always
        // false and EVERY first frame threw "seq out of range: 1".
        // (Python's 2**63 is fine; Kotlin's shl is sign-carrying.)
        // Valid bound: seq < 2^63 == 1L shl 62 * 2, use Long.MAX_VALUE / 2.
        require(seq >= 0 && seq <= (Long.MAX_VALUE) / 2) {
            "seq out of range: $seq" }
        val nonce = dirPrefix + java.nio.ByteBuffer.allocate(8).putLong(seq).array()
        val c = Cipher.getInstance("AES/GCM/NoPadding")
        c.init(Cipher.ENCRYPT_MODE, keySpec, GCMParameterSpec(128, nonce))
        c.updateAAD(aad)
        return c.doFinal(plaintext)
    }

    fun open(seq: Long, ciphertext: ByteArray, aad: ByteArray): ByteArray {
        require(seq >= 0 && seq <= (Long.MAX_VALUE) / 2) {
            "seq out of range: $seq" }
        val nonce = dirPrefix + java.nio.ByteBuffer.allocate(8).putLong(seq).array()
        val c = Cipher.getInstance("AES/GCM/NoPadding")
        c.init(Cipher.DECRYPT_MODE, keySpec, GCMParameterSpec(128, nonce))
        c.updateAAD(aad)
        return c.doFinal(ciphertext)
    }

    fun seenHigher(seq: Long): Boolean = lastSeq.get() >= seq

    /** Raw-payload variants used by the GAS batch envelope (GasCodec):
     *  the caller owns the 4-byte label (distinct from frame prefixes)
     *  and the AAD — mirrors EnvelopeCrypto in core/gas.py. */
    fun sealRaw(counter: Long, plaintext: ByteArray, aad: ByteArray): ByteArray =
        seal(counter, plaintext, aad)

    fun openRaw(ciphertext: ByteArray, counter: Long, aad: ByteArray): ByteArray =
        open(counter, ciphertext, aad)

    companion object {
        fun b64encode(b: ByteArray): String =
            Base64.getUrlEncoder().withoutPadding().encodeToString(b)
        fun b64decode(s: String): ByteArray =
            Base64.getUrlDecoder().decode(s)
    }
}
