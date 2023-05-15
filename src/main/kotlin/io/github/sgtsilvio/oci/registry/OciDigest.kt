package io.github.sgtsilvio.oci.registry

import java.io.Serializable

internal data class OciDigest(val algorithm: String, val hash: String) : Serializable {
    override fun toString() = "$algorithm:$hash"
}

internal fun String.toOciDigest(): OciDigest {
    val separator = indexOf(':')
    if (separator == -1) throw IllegalArgumentException("'$this' is not an OCI digest")
    return OciDigest(substring(0, separator), substring(separator + 1))
}
