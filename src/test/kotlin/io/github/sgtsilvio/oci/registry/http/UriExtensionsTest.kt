package io.github.sgtsilvio.oci.registry.http

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.net.URI

/**
 * @author Silvio Giebl
 */
class UriExtensionsTest {

    @Test
    fun uriQueryParameters_percentEncodedSpaceAndAmpersand() {
        assertEquals(
            mapOf("key1" to listOf("test &value"), "key2" to listOf("a:b/c")),
            URI("?key1=test%20%26value&key2=a:b/c").queryParameters,
        )
    }

    @Test
    fun uriQueryParameters_formUrlEncoded() {
        assertEquals(
            mapOf("key1" to listOf("test &value"), "key2" to listOf("a:b/c")),
            URI("?key1=test+%26value&key2=a%3Ab%2Fc").queryParameters,
        )
    }
}