package io.github.sgtsilvio.oci.registry

import io.netty.handler.codec.http.HttpHeaderNames.*
import io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_OCTET_STREAM
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpResponseStatus.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import reactor.netty.DisposableServer
import reactor.netty.http.client.HttpClient
import reactor.netty.http.server.HttpServer
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.writeBytes
import kotlin.io.path.writeText
import kotlin.random.Random

/**
 * @author Silvio Giebl
 */
class OciRegistryTest {

    @TempDir
    private lateinit var storageDir: Path
    private lateinit var server: DisposableServer

    @BeforeEach
    fun setUp() {
        server = HttpServer.create().handle(OciRegistryHandler(DistributionRegistryStorage(storageDir))).bindNow()
    }

    @AfterEach
    fun tearDown() = server.dispose()

    private fun uri(path: String) = "http://localhost:${server.port()}$path"

    @ParameterizedTest
    @CsvSource(
        value = [
            "GET, /v2/",
            "GET, /v2",
            "HEAD, /v2/",
            "HEAD, /v2",
        ]
    )
    fun getOrHeadV2(method: String, path: String) {
        val response =
            HttpClient.newConnection().request(HttpMethod.valueOf(method)).uri(uri(path)).response().block()!!
        assertEquals(OK, response.status())
        assertEquals("registry/2.0", response.responseHeaders()["Docker-Distribution-API-Version"])
    }

    private inner class ManifestByTagData {
        val repository = "example/repository-" + Random.nextInt()
        val tag = "tag-" + Random.nextInt()
        val mediaType = "mediaType-" + Random.nextInt()
        val manifest = """{"mediaType":"$mediaType"}"""

        init {
            val digestAlgorithm = "alg-" + Random.nextInt()
            val digestHash = "hash-" + Random.nextInt()
            storageDir.resolve("blobs/$digestAlgorithm/${digestHash.substring(0, 2)}/$digestHash")
                .createDirectories()
                .resolve("data")
                .writeText(manifest)
            storageDir.resolve("repositories/$repository/_manifests/tags/$tag/current")
                .createDirectories()
                .resolve("link")
                .writeText("$digestAlgorithm:$digestHash")
        }
    }

    @Test
    fun getManifestByTag() {
        val data = ManifestByTagData()
        val responseBody = HttpClient.newConnection()
            .get()
            .uri(uri("/v2/${data.repository}/manifests/${data.tag}"))
            .responseSingle { response, body ->
                assertEquals(OK, response.status())
                assertEquals(data.mediaType, response.responseHeaders()[CONTENT_TYPE])
                assertEquals(data.manifest.length.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals(2, response.responseHeaders().size())
                body.asString(Charsets.UTF_8)
            }
            .block()
        assertEquals(data.manifest, responseBody)
    }

    @Test
    fun headManifestByTag() {
        val data = ManifestByTagData()
        val responseBody = HttpClient.newConnection()
            .head()
            .uri(uri("/v2/${data.repository}/manifests/${data.tag}"))
            .responseSingle { response, body ->
                assertEquals(OK, response.status())
                assertEquals(data.mediaType, response.responseHeaders()[CONTENT_TYPE])
                assertEquals(data.manifest.length.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals(2, response.responseHeaders().size())
                body.asString(Charsets.UTF_8)
            }
            .block()
        assertNull(responseBody)
    }

    private inner class ManifestByDigestData {
        val repository = "example/repository-" + Random.nextInt()
        val digestAlgorithm = "alg-" + Random.nextInt()
        val digestHash = "hash-" + Random.nextInt()
        val mediaType = "mediaType-" + Random.nextInt()
        val manifest = """{"mediaType":"$mediaType"}"""

        init {
            storageDir.resolve("blobs/$digestAlgorithm/${digestHash.substring(0, 2)}/$digestHash")
                .createDirectories()
                .resolve("data")
                .writeText(manifest)
            storageDir.resolve("repositories/$repository/_manifests/revisions/$digestAlgorithm/$digestHash")
                .createDirectories()
                .resolve("link")
                .writeText("$digestAlgorithm:$digestHash")
        }
    }

    @Test
    fun getManifestByDigest() {
        val data = ManifestByDigestData()
        val responseBody = HttpClient.newConnection()
            .get()
            .uri(uri("/v2/${data.repository}/manifests/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(OK, response.status())
                assertEquals(data.mediaType, response.responseHeaders()[CONTENT_TYPE])
                assertEquals(data.manifest.length.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals(2, response.responseHeaders().size())
                body.asString(Charsets.UTF_8)
            }
            .block()
        assertEquals(data.manifest, responseBody)
    }

    @Test
    fun headManifestByDigest() {
        val data = ManifestByDigestData()
        val responseBody = HttpClient.newConnection()
            .head()
            .uri(uri("/v2/${data.repository}/manifests/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(OK, response.status())
                assertEquals(data.mediaType, response.responseHeaders()[CONTENT_TYPE])
                assertEquals(data.manifest.length.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals(2, response.responseHeaders().size())
                body.asString(Charsets.UTF_8)
            }
            .block()
        assertNull(responseBody)
    }

    @Test
    fun getManifestByDigest_fromOtherRepository_notFound() {
        val data = ManifestByDigestData()
        val responseBody = HttpClient.newConnection()
            .get()
            .uri(uri("/v2/${data.repository}-other/manifests/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(NOT_FOUND, response.status())
                assertEquals("0", response.responseHeaders()[CONTENT_LENGTH])
                assertEquals(1, response.responseHeaders().size())
                body.asString(Charsets.UTF_8)
            }
            .block()
        assertNull(responseBody)
    }

    private inner class BlobData {
        val repository = "example/repository-" + Random.nextInt()
        val digestAlgorithm = "alg-" + Random.nextInt()
        val digestHash = "hash-" + Random.nextInt()
        val blob = ByteArray(256) { it.toByte() }

        init {
            storageDir.resolve("blobs/$digestAlgorithm/${digestHash.substring(0, 2)}/$digestHash")
                .createDirectories()
                .resolve("data")
                .writeBytes(blob)
            storageDir.resolve("repositories/$repository/_layers/$digestAlgorithm/$digestHash")
                .createDirectories()
                .resolve("link")
                .writeText("$digestAlgorithm:$digestHash")
        }
    }

    @Test
    fun getBlob() {
        val data = BlobData()
        val responseBody = HttpClient.newConnection()
            .get()
            .uri(uri("/v2/${data.repository}/blobs/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(OK, response.status())
                assertEquals(APPLICATION_OCTET_STREAM.toString(), response.responseHeaders()[CONTENT_TYPE])
                assertEquals(data.blob.size.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals(2, response.responseHeaders().size())
                body.asByteArray()
            }
            .block()
        assertArrayEquals(data.blob, responseBody)
    }

    @Test
    fun headBlob() {
        val data = BlobData()
        val responseBody = HttpClient.newConnection()
            .head()
            .uri(uri("/v2/${data.repository}/blobs/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(OK, response.status())
                assertEquals(APPLICATION_OCTET_STREAM.toString(), response.responseHeaders()[CONTENT_TYPE])
                assertEquals(data.blob.size.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals(2, response.responseHeaders().size())
                body.asByteArray()
            }
            .block()
        assertNull(responseBody)
    }

    @Test
    fun deleteBlob_methodNotAllowed() {
        val data = BlobData()
        val response = HttpClient.newConnection()
            .delete()
            .uri(uri("/v2/${data.repository}/blobs/${data.digestAlgorithm}:${data.digestHash}"))
            .response()
            .block()!!
        assertEquals(METHOD_NOT_ALLOWED, response.status())
        assertEquals("0", response.responseHeaders()[CONTENT_LENGTH])
        assertEquals(1, response.responseHeaders().size())
    }

    @Test
    fun getBlob_fromOtherRepository_notFound() {
        val data = BlobData()
        val responseBody = HttpClient.newConnection()
            .get()
            .uri(uri("/v2/${data.repository}-other/blobs/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(NOT_FOUND, response.status())
                assertEquals("0", response.responseHeaders()[CONTENT_LENGTH])
                assertEquals(1, response.responseHeaders().size())
                body.asByteArray()
            }
            .block()
        assertNull(responseBody)
    }

    @Test
    fun headBlob_fromOtherRepository_notFound() {
        val data = BlobData()
        val responseBody = HttpClient.newConnection()
            .head()
            .uri(uri("/v2/${data.repository}-other/blobs/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(NOT_FOUND, response.status())
                assertEquals(0, response.responseHeaders().size())
                body.asByteArray()
            }
            .block()
        assertNull(responseBody)
    }

    @Test
    fun getBlobRange() {
        val data = BlobData()
        val first = 3
        val last = 10
        val count = last - first + 1
        val responseBody = HttpClient.newConnection()
            .headers { headers -> headers[RANGE] = "bytes=$first-$last" }
            .get()
            .uri(uri("/v2/${data.repository}/blobs/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(PARTIAL_CONTENT, response.status())
                assertEquals(APPLICATION_OCTET_STREAM.toString(), response.responseHeaders()[CONTENT_TYPE])
                assertEquals(count.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals("bytes $first-$last/${data.blob.size}", response.responseHeaders()[CONTENT_RANGE])
                assertEquals(3, response.responseHeaders().size())
                body.asByteArray()
            }
            .block()
        assertArrayEquals(data.blob.sliceArray(first..last), responseBody)
    }

    @Test
    fun getBlobRange_lastPositionGreaterThanSize() {
        val data = BlobData()
        val first = 25
        val requestedLast = data.blob.size + 10
        val last = data.blob.size - 1
        val count = last - first + 1
        val responseBody = HttpClient.newConnection()
            .headers { headers -> headers[RANGE] = "bytes=$first-$requestedLast" }
            .get()
            .uri(uri("/v2/${data.repository}/blobs/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(PARTIAL_CONTENT, response.status())
                assertEquals(APPLICATION_OCTET_STREAM.toString(), response.responseHeaders()[CONTENT_TYPE])
                assertEquals(count.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals("bytes $first-$last/${data.blob.size}", response.responseHeaders()[CONTENT_RANGE])
                assertEquals(3, response.responseHeaders().size())
                body.asByteArray()
            }
            .block()
        assertArrayEquals(data.blob.sliceArray(first..last), responseBody)
    }

    @Test
    fun getBlobRangeWithOpenEnd() {
        val data = BlobData()
        val first = 25
        val last = data.blob.size - 1
        val count = last - first + 1
        val responseBody = HttpClient.newConnection()
            .headers { headers -> headers[RANGE] = "bytes=$first-" }
            .get()
            .uri(uri("/v2/${data.repository}/blobs/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(PARTIAL_CONTENT, response.status())
                assertEquals(APPLICATION_OCTET_STREAM.toString(), response.responseHeaders()[CONTENT_TYPE])
                assertEquals(count.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals("bytes $first-$last/${data.blob.size}", response.responseHeaders()[CONTENT_RANGE])
                assertEquals(3, response.responseHeaders().size())
                body.asByteArray()
            }
            .block()
        assertArrayEquals(data.blob.sliceArray(first..last), responseBody)
    }

    @Test
    fun getBlobSuffixRange() {
        val data = BlobData()
        val count = 10
        val first = data.blob.size - count
        val last = data.blob.size - 1
        val responseBody = HttpClient.newConnection()
            .headers { headers -> headers[RANGE] = "bytes=-$count" }
            .get()
            .uri(uri("/v2/${data.repository}/blobs/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(PARTIAL_CONTENT, response.status())
                assertEquals(APPLICATION_OCTET_STREAM.toString(), response.responseHeaders()[CONTENT_TYPE])
                assertEquals(count.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals("bytes $first-$last/${data.blob.size}", response.responseHeaders()[CONTENT_RANGE])
                assertEquals(3, response.responseHeaders().size())
                body.asByteArray()
            }
            .block()
        assertArrayEquals(data.blob.sliceArray(first..last), responseBody)
    }

    @Test
    fun getBlobSuffixRange_suffixLengthGreaterThanSize() {
        val data = BlobData()
        val responseBody = HttpClient.newConnection()
            .headers { headers -> headers[RANGE] = "bytes=-${data.blob.size + 10}" }
            .get()
            .uri(uri("/v2/${data.repository}/blobs/${data.digestAlgorithm}:${data.digestHash}"))
            .responseSingle { response, body ->
                assertEquals(PARTIAL_CONTENT, response.status())
                assertEquals(APPLICATION_OCTET_STREAM.toString(), response.responseHeaders()[CONTENT_TYPE])
                assertEquals(data.blob.size.toString(), response.responseHeaders()[CONTENT_LENGTH])
                assertEquals(
                    "bytes 0-${data.blob.size - 1}/${data.blob.size}",
                    response.responseHeaders()[CONTENT_RANGE],
                )
                assertEquals(3, response.responseHeaders().size())
                body.asByteArray()
            }
            .block()
        assertArrayEquals(data.blob, responseBody)
    }
}