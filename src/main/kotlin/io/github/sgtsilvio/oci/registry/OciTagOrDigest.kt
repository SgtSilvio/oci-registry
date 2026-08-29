package io.github.sgtsilvio.oci.registry

/**
 * @author Silvio Giebl
 */
internal sealed interface OciTagOrDigest

internal fun String.toOciTagOrDigest() = if (':' in this) toOciDigest() else toOciTag()

internal class OciTag(val name: String) : OciTagOrDigest, Comparable<OciTag> {

    override fun compareTo(other: OciTag) = name.compareTo(other.name)

    override fun equals(other: Any?) = (this === other) || ((other is OciTag) && (name == other.name))

    override fun hashCode() = name.hashCode()

    override fun toString() = name
}

internal fun String.toOciTag() = OciTag(this)
