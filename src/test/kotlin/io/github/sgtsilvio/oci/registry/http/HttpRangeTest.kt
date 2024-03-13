package io.github.sgtsilvio.oci.registry.http

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * @author Silvio Giebl
 */
class HttpRangeTest {

    @Test
    fun decodeHttpRangeSpecs_singleRange() {
        val rangeSpecs = "3-10".decodeHttpRangeSpecs()
        assertEquals(listOf(HttpRangeSpec(3, 10)), rangeSpecs)
    }

    @Test
    fun decodeHttpRangeSpecs_singleRangeWithFirstEqualToLastPosition() {
        val rangeSpecs = "10-10".decodeHttpRangeSpecs()
        assertEquals(listOf(HttpRangeSpec(10, 10)), rangeSpecs)
    }

    @Test
    fun decodeHttpRangeSpecs_singleRangeWithOpenEnd() {
        val rangeSpecs = "42-".decodeHttpRangeSpecs()
        assertEquals(listOf(HttpRangeSpec(42, -1)), rangeSpecs)
    }

    @Test
    fun decodeHttpRangeSpecs_singleSuffixRange() {
        val rangeSpecs = "-10".decodeHttpRangeSpecs()
        assertEquals(listOf(HttpRangeSpec(-1, 10)), rangeSpecs)
    }

    @Test
    fun decodeHttpRangeSpecs_singleRangeWithSpaces() {
        val rangeSpecs = "  3  -  10  ".decodeHttpRangeSpecs()
        assertEquals(listOf(HttpRangeSpec(3, 10)), rangeSpecs)
    }

    @Test
    fun decodeHttpRangeSpecs_singleRangeWithOpenEndAndSpaces() {
        val rangeSpecs = "  42  -  ".decodeHttpRangeSpecs()
        assertEquals(listOf(HttpRangeSpec(42, -1)), rangeSpecs)
    }

    @Test
    fun decodeHttpRangeSpecs_singleSuffixRangeWithSpaces() {
        val rangeSpecs = "  -  10  ".decodeHttpRangeSpecs()
        assertEquals(listOf(HttpRangeSpec(-1, 10)), rangeSpecs)
    }

    @Test
    fun decodeHttpRangeSpecs_multipleRanges() {
        val rangeSpecs = "3-10,42-,-42".decodeHttpRangeSpecs()
        assertEquals(listOf(HttpRangeSpec(3, 10), HttpRangeSpec(42, -1), HttpRangeSpec(-1, 42)), rangeSpecs)
    }

    @Test
    fun decodeHttpRangeSpecs_multipleRangesWithSpaces() {
        val rangeSpecs = "  3  -  10  ,  42  -  ,  -  42  ".decodeHttpRangeSpecs()
        assertEquals(listOf(HttpRangeSpec(3, 10), HttpRangeSpec(42, -1), HttpRangeSpec(-1, 42)), rangeSpecs)
    }

    @Test
    fun decodeHttpRangeSpecs_empty_throws() {
        assertThrows<IllegalArgumentException> { "".decodeHttpRangeSpecs() }
    }

    @Test
    fun decodeHttpRangeSpecs_onlyHyphen_throws() {
        assertThrows<IllegalArgumentException> { "-".decodeHttpRangeSpecs() }
    }

    @Test
    fun decodeHttpRangeSpecs_missingHyphen_throws() {
        assertThrows<IllegalArgumentException> { "1234".decodeHttpRangeSpecs() }
    }

    @Test
    fun decodeHttpRangeSpecs_multipleHyphens_throws() {
        assertThrows<IllegalArgumentException> { "12--34".decodeHttpRangeSpecs() }
    }

    @Test
    fun decodeHttpRangeSpecs_firstPositionNotANumber_throws() {
        assertThrows<IllegalArgumentException> { "12a-34".decodeHttpRangeSpecs() }
    }

    @Test
    fun decodeHttpRangeSpecs_lastPositionNotANumber_throws() {
        assertThrows<IllegalArgumentException> { "12-34b".decodeHttpRangeSpecs() }
    }

    @Test
    fun decodeHttpRangeSpecs_firstGreaterThanLastPosition_throws() {
        assertThrows<IllegalArgumentException> { "11-10".decodeHttpRangeSpecs() }
    }

    @Test
    fun rangeSpecToString() {
        assertEquals("12-34", HttpRangeSpec(12, 34).toString())
    }

    @Test
    fun rangeSpecToString_rangeWithOpenEnd() {
        assertEquals("12-", HttpRangeSpec(12, -1).toString())
    }

    @Test
    fun rangeSpecToString_suffixRange() {
        assertEquals("-34", HttpRangeSpec(-1, 34).toString())
    }

    @Test
    fun createRange() {
        val range = HttpRangeSpec(11, 22).createRange(33)
        assertEquals(HttpRange(11, 22), range)
    }

    @Test
    fun createRange_lastPositionEqualToSize() {
        val range = HttpRangeSpec(11, 22).createRange(22)
        assertEquals(HttpRange(11, 21), range)
    }

    @Test
    fun createRange_lastPositionGreaterThanSize() {
        val range = HttpRangeSpec(11, 33).createRange(22)
        assertEquals(HttpRange(11, 21), range)
    }

    @Test
    fun createRange_fromRangeWithOpenEnd() {
        val range = HttpRangeSpec(11, -1).createRange(33)
        assertEquals(HttpRange(11, 32), range)
    }

    @Test
    fun createRange_fromSuffixRange() {
        val range = HttpRangeSpec(-1, 22).createRange(33)
        assertEquals(HttpRange(11, 32), range)
    }

    @Test
    fun createRange_fromSuffixRangeWithSuffixLengthGreaterThanSize() {
        val range = HttpRangeSpec(-1, 33).createRange(22)
        assertEquals(HttpRange(0, 21), range)
    }

    @Test
    fun createRange_firstPositionsEqualToSize_throws() {
        assertThrows<IllegalArgumentException> { HttpRangeSpec(11, 33).createRange(11) }
    }

    @Test
    fun createRange_firstPositionsGreaterThanSize_throws() {
        assertThrows<IllegalArgumentException> { HttpRangeSpec(22, 33).createRange(11) }
    }

    @Test
    fun createRange_fromSuffixRangeWithSuffixLength0_throws() {
        assertThrows<IllegalArgumentException> { HttpRangeSpec(-1, 0).createRange(11) }
    }

    @Test
    fun contentRangeHeaderValue() {
        assertEquals("bytes 11-22/33", HttpRange(11, 22).contentRangeHeaderValue(33))
    }
}