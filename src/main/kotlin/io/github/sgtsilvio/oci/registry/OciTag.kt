package io.github.sgtsilvio.oci.registry

/**
 * @author Silvio Giebl
 */
internal sealed interface OciTagOrDigest

internal fun String.toOciTagOrDigest() = if (':' in this) toOciDigest() else toOciTag()

/**
 * @author Silvio Giebl
 */
internal class OciTag(val string: String) : OciTagOrDigest, Comparable<OciTag> {

    init {
        string.validateOciTag()
    }

    override fun compareTo(other: OciTag) = string.compareTo(other.string)

    override fun equals(other: Any?) = (this === other) || ((other is OciTag) && (string == other.string))

    override fun hashCode() = string.hashCode()

    override fun toString() = string
}

internal fun String.toOciTag() = OciTag(this)

/**
 * Validates `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`.
 */
private fun String.validateOciTag(): String {
    if (length !in 1..128) {
        throw IllegalArgumentException("\"$this\" is not a valid OCI tag: it must be between 1 and 128 characters long.")
    }
    if (!this[0].let { it in 'a'..'z' || it in 'A'..'Z' || it in '0'..'9' || it == '_' }) {
        throw IllegalArgumentException("\"$this\" is not a valid OCI tag: the first character must match `[a-zA-Z0-9_]`.")
    }
    for (i in 1..lastIndex) {
        if (!this[i].let { it in 'a'..'z' || it in 'A'..'Z' || it in '0'..'9' || it in "._-" }) {
            throw IllegalArgumentException("\"$this\" is not a valid OCI tag: all characters after the first one must match `[a-zA-Z0-9._-]`.")
        }
    }
    return this
}
