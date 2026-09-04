package io.github.sgtsilvio.oci.registry

/**
 * @author Silvio Giebl
 */
internal class OciRepositoryName(val string: String): Comparable<OciRepositoryName> {

    init {
        string.validateOciRepositoryName()
    }

    override fun compareTo(other: OciRepositoryName) = string.compareTo(other.string)

    override fun equals(other: Any?) = (this === other) || ((other is OciRepositoryName) && (string == other.string))

    override fun hashCode() = string.hashCode()

    override fun toString() = string
}

internal fun String.toOciRepositoryName() = OciRepositoryName(this)

/**
 * Validates `[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*`
 * (can be simplified to `[a-z0-9]+(([/._]|__|-+)[a-z0-9]+)*`).
 */
private fun String.validateOciRepositoryName(): String {
    var i = 0
    while (true) {
        if ((i == length) || !this[i].isOciRepositoryNameAlphanumeric()) {
            throw IllegalArgumentException("\"$this\" is not a valid OCI repository name: the character at index $i must match `[a-z0-9]`")
        }
        do {
            i++
            if (i == length) {
                return this
            }
        } while (this[i].isOciRepositoryNameAlphanumeric())
        when (this[i]) {
            '/', '.' -> i++
            '_' -> {
                i++
                if ((i < length) && (this[i] == '_')) {
                    i++
                }
            }
            '-' -> do i++ while ((i < length) && (this[i] == '-'))
            else -> throw IllegalArgumentException("\"$this\" is not a valid OCI repository name: the character at index $i must match `[a-z0-9._/-]`.")
        }
    }
}

private fun Char.isOciRepositoryNameAlphanumeric() = ((this >= 'a') && (this <= 'z')) || ((this >= '0') && (this <= '9'))
