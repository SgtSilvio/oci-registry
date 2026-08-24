package io.github.sgtsilvio.oci.registry.http

// Specification for range requests: https://www.rfc-editor.org/rfc/rfc9110#name-range-requests

/**
 * Matches `OWS "," OWS` where `OWS = (space | htab)*` (optional whitespace)
 */
private val httpListSeparatorRegex = Regex("[ \u0009]*,[ \u0009]*")

internal fun String.decodeHttpByteRangeSpecs(): List<HttpRangeSpec> {
    val rangeSpecs = split(httpListSeparatorRegex).filter { it.isNotEmpty() }.map { it.decodeHttpByteRangeSpec() }
    if (rangeSpecs.isEmpty()) {
        throw IllegalArgumentException("\"$this\" is not a valid HTTP bytes range spec set, at least one range spec is required.")
    }
    return rangeSpecs
}

private fun String.decodeHttpByteRangeSpec(): HttpRangeSpec {
    val rangeParts = split('-')
    if (rangeParts.size != 2) {
        throw IllegalArgumentException("\"$this\" is not a valid HTTP bytes range spec, it must contain exactly 1 '-' character.")
    }
    val (rangePart1, rangePart2) = rangeParts
    return if (rangePart1.isNotEmpty()) {
        val firstPosition = rangePart1.toLong()
        if (rangePart2.isNotEmpty()) {
            val lastPosition = rangePart2.toLong()
            if (lastPosition < firstPosition) {
                throw IllegalArgumentException("\"$this\" is not a valid HTTP bytes range spec, last position must not be less than first position.")
            }
            HttpRangeSpec(firstPosition, lastPosition)
        } else {
            HttpRangeSpec(firstPosition, -1L)
        }
    } else if (rangePart2.isNotEmpty()) {
        val suffixLength = rangePart2.toLong()
        HttpRangeSpec(-1L, suffixLength)
    } else {
        throw IllegalArgumentException("\"$this\" is not a valid HTTP bytes range spec, it must contain at least a first position or a suffix length.")
    }
}

internal data class HttpRangeSpec(val first: Long, val last: Long) {
    override fun toString() = buildString {
        if (first != -1L) append(first)
        append('-')
        if (last != -1L) append(last)
    }
}

internal fun HttpRangeSpec.createRange(size: Long): HttpRange {
    return if (first == -1L) {
        if (last == 0L) {
            throw IllegalArgumentException("HTTP range spec $this is not satisfiable for resource with size $size, suffix length must not be 0")
        }
        HttpRange(maxOf(0L, size - last), size - 1L)
    } else {
        if (first >= size) {
            throw IllegalArgumentException("HTTP range spec $this is not satisfiable for resource with size $size, first position must be less than $size")
        }
        HttpRange(first, if (last == -1L) size - 1L else minOf(size - 1L, last))
    }
}

internal data class HttpRange(val first: Long, val last: Long) {
    val size get() = last - first + 1L
}

internal fun HttpRange.contentRangeHeaderValue(size: Long) = "bytes $first-$last/$size"
