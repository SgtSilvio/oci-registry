package io.github.sgtsilvio.oci.registry.http

import kotlin.math.max
import kotlin.math.min

// Specification for range requests: https://www.rfc-editor.org/rfc/rfc9110#name-range-requests

internal fun String.decodeHttpRangeSpecs(): List<HttpRangeSpec> = split(',').map { it.decodeHttpRangeSpec() }

private fun String.decodeHttpRangeSpec(): HttpRangeSpec {
    val rangeParts = split('-')
    if (rangeParts.size != 2) {
        throw IllegalArgumentException("\"$this\" is not a valid HTTP range spec, it must contain exactly 1 '-' character.")
    }
    val rangePart1 = rangeParts[0].trim()
    val rangePart2 = rangeParts[1].trim()
    val first = if (rangePart1.isEmpty()) -1 else rangePart1.toLong()
    val last: Long
    if (rangePart2.isEmpty()) {
        if (first == -1L) {
            throw IllegalArgumentException("\"$this\" is not a valid HTTP range spec, it must contain at least a start position or a suffix length.")
        }
        last = -1
    } else {
        last = rangePart2.toLong()
        if (last < first) {
            throw IllegalArgumentException("\"$this\" is not a valid HTTP range spec, last position must not be less than first position.")
        }
    }
    return HttpRangeSpec(first, last)
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
        HttpRange(max(0, size - last), size - 1)
    } else {
        if (first >= size) {
            throw IllegalArgumentException("HTTP range spec $this is not satisfiable for resource with size $size, first position must be less than $size")
        }
        HttpRange(first, if (last == -1L) size - 1 else min(size - 1, last))
    }
}

internal data class HttpRange(val first: Long, val last: Long) {
    val size get() = last - first + 1
}

internal fun HttpRange.contentRangeHeaderValue(size: Long) = "bytes $first-$last/$size"
