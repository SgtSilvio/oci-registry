package io.github.sgtsilvio.oci.registry

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * @author Silvio Giebl
 */
class OciDigestTest {

    @Test
    fun toOciDigest() {
        val digest = "sha256:0123456789012345678901234567890123456789012345678901234567890123".toOciDigest()
        assertEquals("sha256", digest.algorithm)
        assertEquals("0123456789012345678901234567890123456789012345678901234567890123", digest.hash)
    }

    @Test
    fun toOciDigest_unknownAlgorithm() {
        val digest = "foo+bar.wab_47-11:abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ-0123456789=".toOciDigest()
        assertEquals("foo+bar.wab_47-11", digest.algorithm)
        assertEquals("abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ-0123456789=", digest.hash)
    }

    @Test
    fun toOciDigest_withoutColon_throws() {
        assertThrows<IllegalArgumentException> {
            "sha256-0123456789012345678901234567890123456789012345678901234567890123".toOciDigest()
        }
    }
}