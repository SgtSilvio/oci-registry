package io.github.sgtsilvio.oci.registry

import java.io.Serializable

/**
 * @author Silvio Giebl
 */
internal data class OciDigest(val algorithm: String, val hash: String) : Serializable {
    override fun toString() = "$algorithm:$hash"
}

internal fun String.toOciDigest(): OciDigest {
    val colonIndex = indexOf(':')
    if (colonIndex == -1) {
        throw IllegalArgumentException("missing ':' in digest '$this'")
    }
    return OciDigest(substring(0, colonIndex), substring(colonIndex + 1))
}
