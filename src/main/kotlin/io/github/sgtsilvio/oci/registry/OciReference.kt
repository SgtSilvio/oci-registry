package io.github.sgtsilvio.oci.registry

/**
 * @author Silvio Giebl
 */
internal sealed interface OciReference

internal fun String.toOciReference() = if (':' in this) toOciDigest() else toOciTag()

internal class OciTag(val name: String) : OciReference {

    override fun equals(other: Any?) = when {
        this === other -> true
        other !is OciTag -> false
        name != other.name -> false
        else -> true
    }

    override fun hashCode() = name.hashCode()

    override fun toString() = name
}

internal fun String.toOciTag() = OciTag(this)
