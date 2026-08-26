package io.github.sgtsilvio.oci.registry.http

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * @author Silvio Giebl
 */
class HttpRangeTest {

    @Test
    fun decodeHttpByteRangeSpecs_singleIntervalRangeSpec() {
        val rangeSpecs = "3-10".decodeHttpByteRangeSpecs()
        assertEquals(listOf(HttpIntervalRangeSpec(3, 10)), rangeSpecs)
    }

    @Test
    fun decodeHttpByteRangeSpecs_singleIntervalRangeSpecWithFirstEqualToLastPosition() {
        val rangeSpecs = "10-10".decodeHttpByteRangeSpecs()
        assertEquals(listOf(HttpIntervalRangeSpec(10, 10)), rangeSpecs)
    }

    @Test
    fun decodeHttpByteRangeSpecs_singleIntervalRangeSpecWithFirstGreaterThanLastPosition() {
        val rangeSpecs = "11-10".decodeHttpByteRangeSpecs()
        assertEquals(listOf(HttpIntervalRangeSpec(11, 10)), rangeSpecs)
    }

    @Test
    fun decodeHttpByteRangeSpecs_singleUnboundedIntervalRangeSpec() {
        val rangeSpecs = "42-".decodeHttpByteRangeSpecs()
        assertEquals(listOf(HttpIntervalRangeSpec(42, -1)), rangeSpecs)
    }

    @Test
    fun decodeHttpByteRangeSpecs_singleSuffixRangeSpec() {
        val rangeSpecs = "-10".decodeHttpByteRangeSpecs()
        assertEquals(listOf(HttpSuffixRangeSpec(10)), rangeSpecs)
    }

    @Test
    fun decodeHttpByteRangeSpecs_singleIntervalRangeSpecWithSpaces() {
        val rangeSpecs = "  ,  3-10  ,  ".decodeHttpByteRangeSpecs()
        assertEquals(listOf(HttpIntervalRangeSpec(3, 10)), rangeSpecs)
    }

    @Test
    fun decodeHttpByteRangeSpecs_singleUnboundedIntervalSpecRangeAndSpaces() {
        val rangeSpecs = "  ,  42-  ,  ".decodeHttpByteRangeSpecs()
        assertEquals(listOf(HttpIntervalRangeSpec(42, -1)), rangeSpecs)
    }

    @Test
    fun decodeHttpByteRangeSpecs_singleSuffixRangeSpecWithSpaces() {
        val rangeSpecs = "  ,  -10  ,  ".decodeHttpByteRangeSpecs()
        assertEquals(listOf(HttpSuffixRangeSpec(10)), rangeSpecs)
    }

    @Test
    fun decodeHttpByteRangeSpecs_multipleRangeSpecs() {
        val rangeSpecs = "3-10,42-,-42".decodeHttpByteRangeSpecs()
        assertEquals(
            listOf(HttpIntervalRangeSpec(3, 10), HttpIntervalRangeSpec(42, -1), HttpSuffixRangeSpec(42)),
            rangeSpecs,
        )
    }

    @Test
    fun decodeHttpByteRangeSpecs_multipleRangeSpecsWithSpaces() {
        val rangeSpecs = "  ,  3-10  ,  42-  ,  ,,  -42  ,  ".decodeHttpByteRangeSpecs()
        assertEquals(
            listOf(HttpIntervalRangeSpec(3, 10), HttpIntervalRangeSpec(42, -1), HttpSuffixRangeSpec(42)),
            rangeSpecs,
        )
    }

    @Test
    fun decodeHttpByteRangeSpecs_empty_throws() {
        assertThrows<IllegalArgumentException> { "".decodeHttpByteRangeSpecs() }
    }

    @Test
    fun decodeHttpByteRangeSpecs_onlyHyphen_throws() {
        assertThrows<IllegalArgumentException> { "-".decodeHttpByteRangeSpecs() }
    }

    @Test
    fun decodeHttpByteRangeSpecs_missingHyphen_throws() {
        assertThrows<IllegalArgumentException> { "1234".decodeHttpByteRangeSpecs() }
    }

    @Test
    fun decodeHttpByteRangeSpecs_multipleHyphens_throws() {
        assertThrows<IllegalArgumentException> { "12--34".decodeHttpByteRangeSpecs() }
    }

    @Test
    fun decodeHttpByteRangeSpecs_firstPositionNotANumber_throws() {
        assertThrows<IllegalArgumentException> { "12a-34".decodeHttpByteRangeSpecs() }
    }

    @Test
    fun decodeHttpByteRangeSpecs_lastPositionNotANumber_throws() {
        assertThrows<IllegalArgumentException> { "12-34b".decodeHttpByteRangeSpecs() }
    }

    @Test
    fun intervalRangeSpec_toString() {
        assertEquals("12-34", HttpIntervalRangeSpec(12, 34).toString())
    }

    @Test
    fun unboundedIntervalRangeSpec_toString() {
        assertEquals("12-", HttpIntervalRangeSpec(12, -1).toString())
    }

    @Test
    fun suffixRangeSpec_toString() {
        assertEquals("-34", HttpSuffixRangeSpec(34).toString())
    }

    @Test
    fun intervalRangeSpec_createRange() {
        val range = HttpIntervalRangeSpec(11, 22).createRange(33)
        assertEquals(HttpRange(11, 22, 33), range)
    }

    @Test
    fun intervalRangeSpecWithLastPositionEqualToSize_createRange() {
        val range = HttpIntervalRangeSpec(11, 22).createRange(22)
        assertEquals(HttpRange(11, 21, 22), range)
    }

    @Test
    fun intervalRangeSpecWithLastPositionGreaterThanSize_createRange() {
        val range = HttpIntervalRangeSpec(11, 33).createRange(22)
        assertEquals(HttpRange(11, 21, 22), range)
    }

    @Test
    fun unboundedIntervalRangeSpec_createRange() {
        val range = HttpIntervalRangeSpec(11, -1).createRange(33)
        assertEquals(HttpRange(11, 32, 33), range)
    }

    @Test
    fun suffixRangeSpec_createRange() {
        val range = HttpSuffixRangeSpec(22).createRange(33)
        assertEquals(HttpRange(11, 32, 33), range)
    }

    @Test
    fun suffixRangeSpecWithSuffixLengthGreaterThanSize_createRange() {
        val range = HttpSuffixRangeSpec(33).createRange(22)
        assertEquals(HttpRange(0, 21, 22), range)
    }

    @Test
    fun intervalRangeSpecWithFirstGreaterThanLastPosition_createRange_throws() {
        assertThrows<IllegalArgumentException> { HttpIntervalRangeSpec(11, 10).createRange(22) }
    }

    @Test
    fun intervalRangeSpecWithFirstPositionsEqualToSize_createRange_throws() {
        assertThrows<IllegalArgumentException> { HttpIntervalRangeSpec(11, 33).createRange(11) }
    }

    @Test
    fun intervalRangeSpecWithFirstPositionsGreaterThanSize_createRange_throws() {
        assertThrows<IllegalArgumentException> { HttpIntervalRangeSpec(22, 33).createRange(11) }
    }

    @Test
    fun unboundedIntervalRangeSpecWithFirstPositionsGreaterThanSize_createRange_throws() {
        assertThrows<IllegalArgumentException> { HttpIntervalRangeSpec(22, -1).createRange(11) }
    }

    @Test
    fun suffixRangeWithSuffixLength0_createRange_throws() {
        assertThrows<IllegalArgumentException> { HttpIntervalRangeSpec(-1, 0).createRange(11) }
    }

    @Test
    fun httpRange_toString() {
        assertEquals("bytes 11-22/33", HttpRange(11, 22, 33).toString())
    }
}