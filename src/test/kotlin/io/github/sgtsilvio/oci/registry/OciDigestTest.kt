package io.github.sgtsilvio.oci.registry

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * @author Silvio Giebl
 */
class OciDigestTest {

    @Test
    fun stringToOciDigest_sha256() {
        val digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".toOciDigest()
        assertEquals(StandardOciDigestAlgorithm.SHA_256, digest.algorithm)
        assertEquals("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", digest.encodedHash)
        assertFalse(digest.algorithm.isUnsupported())
    }

    @Test
    fun stringToOciDigest_sha256WithHashTooShort_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde".toOciDigest()
        }
        assertEquals(
            "\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde\" is not a valid OCI sha256 digest encoded hash: it must have length 64.",
            exception.message,
        )
    }

    @Test
    fun stringToOciDigest_sha256WithHashTooLong_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0".toOciDigest()
        }
        assertEquals(
            "\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0\" is not a valid OCI sha256 digest encoded hash: it must have length 64.",
            exception.message,
        )
    }

    @Test
    fun stringToOciDigest_sha256WithUppercaseHexChar_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "sha256:0123456789abCdef0123456789abcdef0123456789abcdef0123456789abcdef".toOciDigest()
        }
        assertEquals(
            "\"0123456789abCdef0123456789abcdef0123456789abcdef0123456789abcdef\" is not a valid OCI sha256 digest encoded hash: it must match `[a-f0-9]`.",
            exception.message,
        )
    }

    @Test
    fun stringToOciDigest_sha256WithNonHexChar_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg".toOciDigest()
        }
        assertEquals(
            "\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg\" is not a valid OCI sha256 digest encoded hash: it must match `[a-f0-9]`.",
            exception.message,
        )
    }

    @Test
    fun stringToOciDigest_sha512() {
        val digest = "sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".toOciDigest()
        assertEquals(StandardOciDigestAlgorithm.SHA_512, digest.algorithm)
        assertEquals("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", digest.encodedHash)
        assertFalse(digest.algorithm.isUnsupported())
    }

    @Test
    fun stringToOciDigest_sha512WithHashTooShort_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde".toOciDigest()
        }
        assertEquals(
            "\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde\" is not a valid OCI sha512 digest encoded hash: it must have length 128.",
            exception.message,
        )
    }

    @Test
    fun stringToOciDigest_sha512WithHashTooLong_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0".toOciDigest()
        }
        assertEquals(
            "\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0\" is not a valid OCI sha512 digest encoded hash: it must have length 128.",
            exception.message,
        )
    }

    @Test
    fun stringToOciDigest_sha512WithUppercaseHexChar_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "sha512:0123456789abCdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".toOciDigest()
        }
        assertEquals(
            "\"0123456789abCdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\" is not a valid OCI sha512 digest encoded hash: it must match `[a-f0-9]`.",
            exception.message,
        )
    }

    @Test
    fun stringToOciDigest_sha512WithNonHexChar_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg".toOciDigest()
        }
        assertEquals(
            "\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg\" is not a valid OCI sha512 digest encoded hash: it must match `[a-f0-9]`.",
            exception.message,
        )
    }

    @Test
    fun stringToOciDigest_unknownAlgorithm() {
        val digest = "abcdefg+hijklmn.opqrstu_vwxyz01-23456789:abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ-0123456789=".toOciDigest()
        assertEquals("abcdefg+hijklmn.opqrstu_vwxyz01-23456789", digest.algorithm.id)
        assertEquals("abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ-0123456789=", digest.encodedHash)
        assertTrue(digest.algorithm.isUnsupported())
    }

    @Test
    fun stringToOciDigest_unknownAlgorithmWithDisallowedChar_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "unKnown-alg:hash".toOciDigest()
        }
        assertEquals(
            "\"unKnown-alg\" is not a valid OCI digest algorithm: it must match `[a-z0-9]+(?:[.+_-][a-z0-9]+)*`.",
            exception.message,
        )
    }

    @Test
    fun stringToOciDigest_unknownAlgorithmWithDisallowedCharInHash_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "unknown-alg:hash%".toOciDigest()
        }
        assertEquals(
            "\"hash%\" is not a valid OCI digest encoded hash: it must match `[a-zA-Z0-9=_-]+`.",
            exception.message,
        )
    }

    @Test
    fun stringToOciDigest_withoutColon_throws() {
        val exception = assertThrows<IllegalArgumentException> {
            "sha256-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".toOciDigest()
        }
        assertEquals(
            "\"sha256-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\" is not a valid OCI digest: it must contain a ':' character.",
            exception.message,
        )
    }
}