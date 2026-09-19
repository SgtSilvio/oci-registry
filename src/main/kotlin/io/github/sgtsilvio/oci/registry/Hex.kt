package io.github.sgtsilvio.oci.registry

/**
 * Lookup table from a 4 bit value to the corresponding lowercase hexadecimal ASCII character (`[0-9a-f]`).
 */
private val NIBBLE_TO_HEX_CHAR = ByteArray(16) { n -> (if (n < 10) '0'.code + n else 'a'.code - 10 + n).toByte() }

/**
 * Lookup table from an 8 bit value to the corresponding 2 lowercase hexadecimal ASCII characters (`[0-9a-f]`).
 */
private val BYTE_TO_HEX_CHARS = ShortArray(256) { b ->
    (NIBBLE_TO_HEX_CHAR[b and 0xF].toInt().shl(8) or NIBBLE_TO_HEX_CHAR[b shr 4].toInt()).toShort()
}

internal fun ByteArray.toHexString(): String {
    val stringBytes = ByteArray(size shl 1)
    var charIndex = 0
    for (b in this) {
        val chars = BYTE_TO_HEX_CHARS[b.toInt() and 0xFF]
        stringBytes[charIndex++] = chars.toByte()
        stringBytes[charIndex++] = chars.toInt().shr(8).toByte()
    }
    return String(stringBytes, Charsets.ISO_8859_1)
}

/**
 * Lookup table from a lowercase hexadecimal ASCII character (`[0-9a-f]`) to the corresponding 4 bit value or `-1` for all other Latin-1 characters.
 */
private val HEX_CHAR_TO_NIBBLE = ByteArray(256) { -1 }.apply {
    NIBBLE_TO_HEX_CHAR.forEachIndexed { n, c -> this[c.toInt()] = n.toByte() }
}

/**
 * Lookup function from a lowercase hexadecimal ASCII character (`[0-9a-f]`) to the corresponding 4 bit value or `-1` for all other UTF-16 code units.
 */
private fun Char.hexToNibble(): Byte = if (code.ushr(8) == 0) HEX_CHAR_TO_NIBBLE[code] else -1

internal fun String.hexToByteArray(): ByteArray {
    if ((length and 1) != 0) {
        throw IllegalArgumentException("\"$this\" is not a valid hexadecimal string: hexadecimal characters must exist in pairs.")
    }
    var charIndex = 0
    return ByteArray(length shr 1) {
        val high = this[charIndex++].hexToNibble()
        val low = this[charIndex++].hexToNibble()
        val i = high.toInt().shl(4) or low.toInt()
        if (i < 0) {
            throw IllegalArgumentException("\"$this\" is not a valid hexadecimal string: must only contain `[0-9a-f]`.")
        }
        i.toByte()
    }
}
