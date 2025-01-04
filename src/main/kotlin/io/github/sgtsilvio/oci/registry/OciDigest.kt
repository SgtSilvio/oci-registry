package io.github.sgtsilvio.oci.registry

import org.apache.commons.codec.binary.Hex
import java.security.MessageDigest

/**
 * @author Silvio Giebl
 */
internal class OciDigest(val algorithm: OciDigestAlgorithm, val hash: ByteArray) {
    val encodedHash get() = algorithm.encodeHash(hash)

    init {
        algorithm.validateHash(hash)
    }

    override fun equals(other: Any?) = when {
        this === other -> true
        other !is OciDigest -> false
        algorithm != other.algorithm -> false
        !hash.contentEquals(other.hash) -> false
        else -> true
    }

    override fun hashCode(): Int {
        var result = algorithm.hashCode()
        result = 31 * result + hash.contentHashCode()
        return result
    }

    override fun toString() = "${algorithm.id}:$encodedHash"
}

internal interface OciDigestAlgorithm {
    val id: String

    fun encodeHash(hash: ByteArray): String

    fun decodeHash(encodedHash: String): ByteArray

    fun validateHash(hash: ByteArray): ByteArray

    fun validateEncodedHash(encodedHash: String): String

    fun createMessageDigest(): MessageDigest
}

// id: https://github.com/opencontainers/image-spec/blob/main/descriptor.md#registered-algorithms
// hashAlgorithmName: https://docs.oracle.com/en/java/javase/21/docs/specs/security/standard-names.html#messagedigest-algorithms
internal enum class StandardOciDigestAlgorithm(
    override val id: String,
    private val hashAlgorithmName: String,
    private val hashByteLength: Int,
) : OciDigestAlgorithm {
    SHA_256("sha256", "SHA-256", 32),
    SHA_512("sha512", "SHA-512", 64);

    override fun encodeHash(hash: ByteArray): String = Hex.encodeHexString(validateHash(hash))

    override fun decodeHash(encodedHash: String): ByteArray = Hex.decodeHex(validateEncodedHash(encodedHash))

    override fun validateHash(hash: ByteArray): ByteArray {
        if (hash.size != hashByteLength) {
            throw IllegalArgumentException("digest hash has wrong length ${hash.size}, $hashAlgorithmName requires $hashByteLength")
        }
        return hash
    }

    override fun validateEncodedHash(encodedHash: String): String {
        if (encodedHash.length != (hashByteLength * 2)) {
            throw IllegalArgumentException("digest encoded hash '$encodedHash' has wrong length ${encodedHash.length}, $hashAlgorithmName requires ${hashByteLength * 2}")
        }
        if (!encodedHash.all { c -> ((c >= '0') && (c <= '9')) || ((c >= 'a') && (c <= 'f')) }) {
            throw IllegalArgumentException("digest encoded hash '$encodedHash' does not match [a-f0-9]")
        }
        return encodedHash
    }

    override fun createMessageDigest(): MessageDigest = MessageDigest.getInstance(hashAlgorithmName)

    override fun toString() = id
}

private val OCI_DIGEST_ALGORITHM_REGEX = Regex("[a-z0-9]+(?:[.+_-][a-z0-9]+)*")
private val OCI_DIGEST_ENCODED_HASH_REGEX = Regex("[a-zA-Z0-9=_-]+")

private class UnsupportedOciDigestAlgorithm(override val id: String) : OciDigestAlgorithm {

    init {
        if (!OCI_DIGEST_ALGORITHM_REGEX.matches(id)) {
            throw IllegalArgumentException("digest algorithm '$id' does not match $OCI_DIGEST_ALGORITHM_REGEX")
        }
    }

    override fun encodeHash(hash: ByteArray) = validateEncodedHash(hash.toString(Charsets.ISO_8859_1))

    override fun decodeHash(encodedHash: String) = validateEncodedHash(encodedHash).toByteArray(Charsets.ISO_8859_1)

    override fun validateHash(hash: ByteArray): ByteArray {
        encodeHash(hash)
        return hash
    }

    override fun validateEncodedHash(encodedHash: String): String {
        if (!OCI_DIGEST_ENCODED_HASH_REGEX.matches(encodedHash)) {
            throw IllegalArgumentException("digest encoded hash '$encodedHash' does not match $OCI_DIGEST_ENCODED_HASH_REGEX")
        }
        return encodedHash
    }

    override fun createMessageDigest() = throw UnsupportedOperationException("unsupported digest algorithm '$id'")

    override fun equals(other: Any?) = when {
        this === other -> true
        other !is UnsupportedOciDigestAlgorithm -> false
        id != other.id -> false
        else -> true
    }

    override fun hashCode(): Int = id.hashCode()

    override fun toString() = id
}

internal fun String.toOciDigest(): OciDigest {
    val colonIndex = indexOf(':')
    if (colonIndex == -1) {
        throw IllegalArgumentException("missing ':' in digest '$this'")
    }
    val algorithm = when (val algorithmId = substring(0, colonIndex)) {
        StandardOciDigestAlgorithm.SHA_256.id -> StandardOciDigestAlgorithm.SHA_256
        StandardOciDigestAlgorithm.SHA_512.id -> StandardOciDigestAlgorithm.SHA_512
        else -> UnsupportedOciDigestAlgorithm(algorithmId)
    }
    return OciDigest(algorithm, algorithm.decodeHash(substring(colonIndex + 1)))
}

internal fun ByteArray.calculateOciDigest(algorithm: OciDigestAlgorithm) =
    OciDigest(algorithm, algorithm.createMessageDigest().digest(this))
