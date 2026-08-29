package io.github.sgtsilvio.oci.registry.http

/**
 * Decodes `[0-9]+` into a `Long`, no `+` or `-` signs allowed.
 */
internal fun String.decodeLongWithoutSign() = validateUnsignedInteger().toLong()

/**
 * Decodes `[0-9]+` into an `Int`, no `+` or `-` signs allowed.
 */
internal fun String.decodeIntWithoutSign() = validateUnsignedInteger().toInt()

/**
 * Validates `[0-9]+`.
 */
private fun String.validateUnsignedInteger(): String {
    if (isEmpty() || !all { it in '0'..'9' }) {
        throw IllegalArgumentException("\"$this\" does not match [0-9]+")
    }
    return this
}
