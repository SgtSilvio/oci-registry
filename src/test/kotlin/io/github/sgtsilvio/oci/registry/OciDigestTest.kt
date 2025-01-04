package io.github.sgtsilvio.oci.registry

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * @author Silvio Giebl
 */
class OciDigestTest {

    @Test
    fun stringToOciDigest() {
        val digest = "sha256:0123456789012345678901234567890123456789012345678901234567890123".toOciDigest()
        assertEquals(StandardOciDigestAlgorithm.SHA_256, digest.algorithm)
        assertEquals("0123456789012345678901234567890123456789012345678901234567890123", digest.encodedHash)
    }

    @Test
    fun stringToOciDigest_unknownAlgorithm() {
        val digest = "foo+bar.wab_47-11:abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ-0123456789=".toOciDigest()
        assertEquals("foo+bar.wab_47-11", digest.algorithm.id)
        assertEquals("abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ-0123456789=", digest.encodedHash)
    }

    @Test
    fun stringToOciDigest_withoutColon_throws() {
        assertThrows<IllegalArgumentException> {
            "sha256-0123456789012345678901234567890123456789012345678901234567890123".toOciDigest()
        }
    }
}