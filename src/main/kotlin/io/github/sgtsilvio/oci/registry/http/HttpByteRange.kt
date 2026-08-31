package io.github.sgtsilvio.oci.registry.http

// Specification for range requests: https://www.rfc-editor.org/rfc/rfc9110#name-range-requests

/**
 * Matches `OWS "," OWS` where `OWS = (space | htab)*` (optional whitespace)
 */
private val httpListSeparatorRegex = Regex("[ \u0009]*,[ \u0009]*")

/**
 * Decodes an HTTP byte range spec set (part of a `content-range` HTTP header with the `bytes` range unit) according to RFC 9110.
 * ```
 * byte-range-spec-set = 1#byte-range-spec
 * ```
 */
internal fun String.decodeHttpByteRangeSpecs(): List<HttpByteRangeSpec> {
    val rangeSpecs = split(httpListSeparatorRegex).filter { it.isNotEmpty() }.map { it.decodeHttpByteRangeSpec() }
    if (rangeSpecs.isEmpty()) {
        throw IllegalArgumentException("\"$this\" is not a valid HTTP bytes range spec set: at least one range spec is required.")
    }
    return rangeSpecs
}

/**
 * Decodes an HTTP byte range spec (part of a `content-range` HTTP header with the `bytes` range unit) according to RFC 9110.
 * ```
 * byte-range-spec = interval-byte-range-spec | suffix-byte-range-spec
 * interval-byte-range-spec = first-position "-" [ last-position ]
 * suffix-byte-range-spec = "-" suffix-length
 * first-position / last-position / suffix-length = [0-9]*
 * ```
 */
private fun String.decodeHttpByteRangeSpec(): HttpByteRangeSpec {
    val rangeParts = split('-')
    if (rangeParts.size != 2) {
        throw IllegalArgumentException("\"$this\" is not a valid HTTP bytes range spec: it must contain exactly one '-' character.")
    }
    val (rangePart1, rangePart2) = rangeParts
    return if (rangePart1.isNotEmpty()) {
        val firstPosition = rangePart1.decodeLongWithoutSign()
        val lastPosition = if (rangePart2.isNotEmpty()) rangePart2.decodeLongWithoutSign() else -1L
        HttpIntervalByteRangeSpec(firstPosition, lastPosition)
    } else if (rangePart2.isNotEmpty()) {
        val suffixLength = rangePart2.decodeLongWithoutSign()
        HttpSuffixByteRangeSpec(suffixLength)
    } else {
        throw IllegalArgumentException("\"$this\" is not a valid HTTP bytes range spec: it must contain at least a first position or a suffix length.")
    }
}

internal sealed interface HttpByteRangeSpec {
    fun createRange(size: Long): HttpByteRange
}

internal class HttpIntervalByteRangeSpec(val first: Long, val last: Long) : HttpByteRangeSpec {

    init {
        require(first >= 0L)
        require(last >= -1L)
    }

    override fun createRange(size: Long): HttpByteRange {
        if ((last != -1L) && (last < first)) {
            throw IllegalArgumentException("HTTP byte range spec $this is not satisfiable: last position must not be less than first position.")
        }
        if (first >= size) {
            throw IllegalArgumentException("HTTP byte range spec $this is not satisfiable: first position must be less than resource size $size.")
        }
        return HttpByteRange(first, if (last == -1L) size - 1L else last.coerceAtMost(size - 1L), size)
    }

    override fun equals(other: Any?) =
        (this === other) || ((other is HttpIntervalByteRangeSpec) && (first == other.first) && (last == other.last))

    override fun hashCode() = first.hashCode() * 31 + last.hashCode()

    override fun toString() = if (last == -1L) "$first-" else "$first-$last"
}

internal class HttpSuffixByteRangeSpec(val suffixLength: Long) : HttpByteRangeSpec {

    init {
        require(suffixLength >= 0L)
    }

    override fun createRange(size: Long): HttpByteRange {
        if (suffixLength == 0L) {
            throw IllegalArgumentException("HTTP byte range spec $this is not satisfiable: suffix length must not be 0.")
        }
        return HttpByteRange((size - suffixLength).coerceAtLeast(0L), size - 1L, size)
    }

    override fun equals(other: Any?) =
        (this === other) || ((other is HttpSuffixByteRangeSpec) && (suffixLength == other.suffixLength))

    override fun hashCode() = suffixLength.hashCode()

    override fun toString() = "-$suffixLength"
}

internal class HttpByteRange(val first: Long, val last: Long, val completeSize: Long) {

    init {
        require(first >= 0L)
        require(first <= last)
        require(last < completeSize)
    }

    val size get() = last - first + 1L

    override fun equals(other: Any?) =
        (this === other) || ((other is HttpByteRange) && (first == other.first) && (last == other.last) && (completeSize == other.completeSize))

    override fun hashCode() = (first.hashCode() * 31 + last.hashCode()) * 31 + completeSize.hashCode()

    override fun toString() = "bytes $first-$last/$completeSize"
}

internal fun encodeHttpUnsatisfiedByteRangeHeaderValue(size: Long) = "bytes */$size"
