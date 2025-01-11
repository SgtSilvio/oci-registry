package io.github.sgtsilvio.oci.registry

import io.github.sgtsilvio.oci.registry.http.*
import io.netty.handler.codec.http.HttpHeaderNames.*
import io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_OCTET_STREAM
import io.netty.handler.codec.http.HttpMethod.*
import io.netty.handler.codec.http.HttpResponseStatus.*
import org.json.JSONException
import org.json.JSONObject
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.server.HttpServerResponse
import java.net.URI
import java.security.DigestException
import java.util.function.BiFunction
import kotlin.io.path.fileSize

/**
 * | resource                                                    | method | response codes | spec reference | category           |
 * |-------------------------------------------------------------|--------|----------------|----------------|--------------------|
 * | `/v2`, `/v2/`                                               | GET    | 200            | end-1          |                    |
 * | `/v2`, `/v2/`                                               | HEAD   | 200            |                |                    |
 * | `/v2/<name>/manifests/<reference>`                          | GET    | 200, 404       | end-3          | pull               |
 * | `/v2/<name>/manifests/<reference>`                          | HEAD   | 200, 404       | end-3          | pull               |
 * | `/v2/<name>/manifests/<reference>`                          | PUT    | 201            | end-7          | push               |
 * | `/v2/<name>/manifests/<reference>`                          | DELETE | 405            | end-9          | content management |
 * | `/v2/<name>/blobs/<digest>`                                 | GET    | 200, 404       | end-2          | pull               |
 * | `/v2/<name>/blobs/<digest>`                                 | HEAD   | 200, 404       | end-2          | pull               |
 * | `/v2/<name>/blobs/<digest>`                                 | DELETE | 405            | end-10         | content management |
 * | `/v2/<name>/blobs/uploads/`                                 | POST   | 202            | end-4a         | push               |
 * | `/v2/<name>/blobs/uploads/?mount=<digest>&from=<otherName>` | POST   | 201, 202       | end-11         | push               |
 * | `/v2/<name>/blobs/uploads/?digest=<digest>`                 | POST   | 201, 202       | end-4b         | push               |
 * | `/v2/<name>/blobs/uploads/<reference>`                      | GET    | 204, 404       | end-13         | push               |
 * | `/v2/<name>/blobs/uploads/<reference>`                      | HEAD   | 204, 404       |                | push               |
 * | `/v2/<name>/blobs/uploads/<reference>`                      | PATCH  | 202, 404       | end-5          | push               |
 * | `/v2/<name>/blobs/uploads/<reference>`                      | DELETE | 405            |                | push               |
 * | `/v2/<name>/blobs/uploads/<reference>?digest=<digest>`      | PUT    | 201, 404       | end-6          | push               |
 * | `/v2/<name>/tags/list`                                      | GET    | 405            | end-8a         | content discovery  |
 * | `/v2/<name>/tags/list?n=<integer>[&last=<tagName>]`         | GET    | 405            | end-8b         | content discovery  |
 * | `/v2/<name>/referrers/<digest>`                             | GET    | 404            | end-12a        | content discovery  |
 * | `/v2/<name>/referrers/<digest>?artifactType=<artifactType>` | GET    | 404            | end-12b        | content discovery  |
 * | `/v2/_catalog`                                              | GET    | 405            |                | content discovery  |
 * | `/v2/_catalog?n=<integer>[&last=<repositoryName>]`          | GET    | 405            |                | content discovery  |
 *
 * [OCI Distribution Specification](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)
 *
 * @author Silvio Giebl
 */
class OciRegistryHandler(
    private val storage: OciRegistryStorage,
) : BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

    override fun apply(request: HttpServerRequest, response: HttpServerResponse): Publisher<Void> {
        val path = request.fullPath().substring(1)
        return when (path.substringBefore('/')) {
            "v2" -> handleV2(request, path.substring("v2".length), response)
            else -> response.sendNotFound()
        }
    }

    private fun handleV2(request: HttpServerRequest, path: String, response: HttpServerResponse): Publisher<Void> {
        when (path) {
            "", "/" -> return when (request.method()) {
                GET, HEAD -> response.header("Docker-Distribution-API-Version", "registry/2.0").send()
                else -> response.status(METHOD_NOT_ALLOWED).send()
            }

            "/_catalog" -> return handleCatalog(request, response)
        }
        val lastSlashIndex = path.lastIndexOf('/')
        val secondLastSlashIndex = path.lastIndexOf('/', lastSlashIndex - 1)
        if (secondLastSlashIndex < 1) {
            return response.sendNotFound()
        }
        val firstSegments = path.substring(1, secondLastSlashIndex)
        val secondLastSegment = path.substring(secondLastSlashIndex + 1, lastSlashIndex)
        val lastSegment = path.substring(lastSlashIndex + 1)
        return when (secondLastSegment) {
            "tags" -> when (lastSegment) {
                "list" -> handleTags(firstSegments, request, response)
                else -> response.sendNotFound()
            }

            "manifests" -> handleManifest(firstSegments, lastSegment, request, response)
            "blobs" -> handleBlob(firstSegments, lastSegment, request, response)
            "uploads" -> when {
                firstSegments.endsWith("/blobs") -> handleBlobUpload(
                    firstSegments.removeSuffix("/blobs"), lastSegment, request, response
                )

                else -> response.sendNotFound()
            }

            else -> response.sendNotFound()
        }
    }

    private fun handleCatalog(
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> = when (request.method()) {
        GET -> getRepositories(response)
        else -> response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun getRepositories(response: HttpServerResponse): Publisher<Void> {
        return response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun handleTags(
        repositoryName: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> = when (request.method()) {
        GET -> getTags(repositoryName, response)
        else -> response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun getTags(repositoryName: String, response: HttpServerResponse): Publisher<Void> {
//        status: 200, or 404
//        response.header(CONTENT_TYPE, APPLICATION_JSON)
//        body: {
//          "name": "<name>",
//          "tags": [
//            "<tag1>",
//            "<tag2>",
//            "<tag3>"
//          ]
//        }
//        If the list is not empty, the tags MUST be in lexical order (i.e. case-insensitive alphanumeric order).
//        TODO ?n=<integer>&last=<tag name>
        return response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun handleManifest(
        repositoryName: String,
        reference: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> = when (request.method()) {
        GET -> getOrHeadManifest(repositoryName, reference, true, response)
        HEAD -> getOrHeadManifest(repositoryName, reference, false, response)
        PUT -> putManifest(repositoryName, reference, request, response)
        DELETE -> deleteManifest(repositoryName, reference, response)
        else -> response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun getOrHeadManifest(
        repositoryName: String,
        reference: String,
        isGet: Boolean,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val manifestBytes = if (':' in reference) {
            val digest = try {
                reference.toOciDigest()
            } catch (e: IllegalArgumentException) {
                return response.sendBadRequest()
            }
            storage.getManifest(repositoryName, digest)
        } else {
            storage.getManifest(repositoryName, reference)
        } ?: return response.sendNotFound()
        response.header(CONTENT_TYPE, JSONObject(manifestBytes.decodeToString()).getString("mediaType"))
        response.header(CONTENT_LENGTH, manifestBytes.size.toString())
        return if (isGet) response.sendByteArray(Mono.just(manifestBytes)) else response.send()
    }

    private fun putManifest(
        repositoryName: String,
        reference: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val digest: OciDigest?
        val tag: String?
        if (':' in reference) {
            digest = try {
                reference.toOciDigest()
            } catch (e: IllegalArgumentException) {
                return response.sendBadRequest()
            }
            tag = null
        } else {
            digest = null
            tag = reference
        }
        val contentType = request.requestHeaders()[CONTENT_TYPE]
        return request.receive().aggregate().asByteArray().flatMap { data ->
            putManifest(repositoryName, digest, tag, contentType, data, response)
        }
    }

    private fun putManifest(
        repositoryName: String,
        digest: OciDigest?,
        tag: String?,
        mediaType: String?,
        data: ByteArray,
        response: HttpServerResponse,
    ): Mono<Void> {
        val actualDigest = try {
            data.calculateOciDigest(digest?.algorithm ?: StandardOciDigestAlgorithm.SHA_256)
        } catch (e: UnsupportedOperationException) {
            return response.sendBadRequest()
        }
        if ((digest != null) && (digest != actualDigest)) {
            return response.sendBadRequest()
        }
        val manifestJsonObject = try {
            JSONObject(data.decodeToString())
        } catch (e: JSONException) {
            return response.sendBadRequest()
        }
        val actualMediaType = manifestJsonObject.opt("mediaType")
        if (actualMediaType !is String) {
            return response.sendBadRequest()
        }
        if ((mediaType != null) && (mediaType != actualMediaType)) {
            return response.sendBadRequest()
        }
//        when (actualMediaType) {
//            OCI_IMAGE_INDEX_MEDIA_TYPE, DOCKER_MANIFEST_LIST_MEDIA_TYPE -> // TODO validate manifest presence in manifest[].digest (size?)
//            OCI_IMAGE_MANIFEST_MEDIA_TYPE, DOCKER_MANIFEST_MEDIA_TYPE -> // TODO validate blob presence in config.digest and layers[].digest (size?)
//            else -> return response.sendBadRequest()
//        }
        // TODO validate manifests json structure ignoring additional fields
        storage.putManifest(repositoryName, actualDigest, data)
        if (tag != null) {
            storage.tagManifest(repositoryName, actualDigest, tag)
        }
        response.header(LOCATION, "/v2/$repositoryName/manifests/${tag ?: actualDigest}")
        response.header("docker-content-digest", actualDigest.toString())
        return response.status(CREATED).send()
    }

    private fun deleteManifest(
        repositoryName: String,
        reference: String,
        response: HttpServerResponse,
    ): Publisher<Void> {
        return response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun handleBlob(
        repositoryName: String,
        rawDigest: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val digest = try {
            rawDigest.toOciDigest()
        } catch (e: IllegalArgumentException) {
            return response.sendBadRequest()
        }
        return when (request.method()) {
            GET -> getBlob(repositoryName, digest, request, response)
            HEAD -> headBlob(repositoryName, digest, response)
            DELETE -> deleteBlob(repositoryName, digest, response)
            else -> response.status(METHOD_NOT_ALLOWED).send()
        }
    }

    private fun getBlob(
        repositoryName: String,
        digest: OciDigest,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val blobFile = storage.getBlob(repositoryName, digest) ?: return response.sendNotFound()
        val size = blobFile.fileSize()
        val rangeHeader: String? = request.requestHeaders()[RANGE]
        if ((rangeHeader != null) && rangeHeader.startsWith("bytes=")) {
            val rangeSpecs = try {
                rangeHeader.substring("bytes=".length).decodeHttpRangeSpecs()
            } catch (e: IllegalArgumentException) {
                return response.sendBadRequest()
            }
            if (rangeSpecs.size == 1) {
                val range = try {
                    rangeSpecs[0].createRange(size)
                } catch (e: IllegalArgumentException) {
                    return response.sendRangeNotSatisfiable(size)
                }
                response.header(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
                response.header(CONTENT_LENGTH, range.size.toString())
                response.header(CONTENT_RANGE, range.contentRangeHeaderValue(size))
                return response.status(PARTIAL_CONTENT).sendFile(blobFile, range.first, range.size)
            }
        }
        response.header(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
        response.header(CONTENT_LENGTH, size.toString())
        return response.sendFile(blobFile, 0, size)
    }

    private fun headBlob(repositoryName: String, digest: OciDigest, response: HttpServerResponse): Publisher<Void> {
        val blobFile = storage.getBlob(repositoryName, digest) ?: return response.sendNotFound()
        response.header(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
        response.header(CONTENT_LENGTH, blobFile.fileSize().toString())
        return response.send()
    }

    private fun deleteBlob(repositoryName: String, digest: OciDigest, response: HttpServerResponse): Publisher<Void> {
        return response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun handleBlobUpload(
        repositoryName: String,
        id: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> = when (id) {
        "" -> when (request.method()) {
            POST -> postBlobUpload(repositoryName, request, response)
            else -> response.status(METHOD_NOT_ALLOWED).send()
        }

        else -> when (request.method()) {
            GET, HEAD -> getOrHeadBlobUpload(repositoryName, id, response)
            PATCH -> patchBlobUpload(repositoryName, id, request, response)
            PUT -> putBlobUpload(repositoryName, id, request, response)
            DELETE -> deleteBlobUpload(repositoryName, id, response)
            else -> response.status(METHOD_NOT_ALLOWED).send()
        }
    }

    private fun postBlobUpload(
        repositoryName: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val queryParameters = URI(request.uri()).queryParameters
        val mountParameter = queryParameters["mount"]
        val fromParameter = queryParameters["from"]
        if ((mountParameter != null) && (fromParameter != null)) {
            return mountBlob(repositoryName, mountParameter, fromParameter, response)
        }
        val digestParameter = queryParameters["digest"]
        if (digestParameter != null) {
            return putBlob(repositoryName, digestParameter, request, response)
        }
        return createBlobUpload(repositoryName, response)
    }

    private fun mountBlob(
        repositoryName: String,
        rawDigest: String,
        fromRepositoryName: String,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val digest = try {
            rawDigest.toOciDigest()
        } catch (e: IllegalArgumentException) {
            return response.sendBadRequest()
        }
        return if (storage.mountBlob(repositoryName, digest, fromRepositoryName)) {
            response.sendBlobCreated(repositoryName, digest)
        } else {
            createBlobUpload(repositoryName, response)
        }
    }

    private fun putBlob(
        repositoryName: String,
        rawDigest: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val digest = try {
            rawDigest.toOciDigest()
        } catch (e: IllegalArgumentException) {
            return response.sendBadRequest()
        }
//        val contentLengthHeader = request.requestHeaders()[CONTENT_LENGTH]
        val contentType = request.requestHeaders()[CONTENT_TYPE]
        if ((contentType != null) && (contentType != APPLICATION_OCTET_STREAM.toString())) {
            return response.sendBadRequest()
        }
        val id = storage.createBlobUpload(repositoryName)
        return storage.finishBlobUpload(repositoryName, id, request.receive(), 0, digest).flatMap {
            response.sendBlobCreated(repositoryName, digest)
        }.onErrorResume { error -> // TODO error handling after flatMap not 100% correct, would also handle errors from the flattened mono
            when (error) {
                is DigestException -> response.sendBadRequest()
                else -> throw error
            }
        }
    }

    private fun createBlobUpload(repositoryName: String, response: HttpServerResponse): Publisher<Void> {
        val id = storage.createBlobUpload(repositoryName)
        response.header(LOCATION, "/v2/$repositoryName/blobs/uploads/$id")
        return response.status(ACCEPTED).send()
    }

    private fun getOrHeadBlobUpload(repositoryName: String, id: String, response: HttpServerResponse): Publisher<Void> {
        val size = storage.getBlobUploadSize(repositoryName, id) ?: return response.sendNotFound()
        response.header(LOCATION, "/v2/$repositoryName/blobs/uploads/$id")
        response.header(RANGE, "0-${size - 1}")
        return response.status(NO_CONTENT).send()
    }

    // TODO exclusive access for writing (path/putBlobUpload), try lock else respond 416 range not satisfiable

    private fun patchBlobUpload(
        repositoryName: String,
        id: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val requestHeaders = request.requestHeaders()
//        val contentLengthHeader = requestHeaders[CONTENT_LENGTH]
        val contentRange = try {
            // content-range header is required in spec, but docker sends PATCH without range
            requestHeaders[CONTENT_RANGE]?.decodeRange()
        } catch (e: IllegalArgumentException) {
            return response.sendBadRequest()
        }
        val contentType = requestHeaders[CONTENT_TYPE]
        if ((contentType != null) && (contentType != APPLICATION_OCTET_STREAM.toString())) {
            return response.sendBadRequest()
        }
        val offset = if (contentRange == null) 0 else {
            val currentSize = storage.getBlobUploadSize(repositoryName, id) ?: return response.sendNotFound()
            if (contentRange.first != currentSize) {
                return response.status(REQUESTED_RANGE_NOT_SATISFIABLE).send()
            }
            currentSize
        }
        return storage.progressBlobUpload(repositoryName, id, request.receive(), offset).flatMap { size ->
            response.header(LOCATION, "/v2/$repositoryName/blobs/uploads/$id")
            response.header(RANGE, "0-${size - 1}")
            response.status(ACCEPTED).send()
        }.onErrorResume { error -> // TODO error handling after flatMap not 100% correct, would also handle errors from the flattened mono
            when (error) {
                is NoSuchElementException -> response.sendNotFound()
                else -> throw error
            }
        }
    }

    private fun putBlobUpload(
        repositoryName: String,
        id: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val queryParameters = URI(request.uri()).queryParameters
        val digestParameter = queryParameters["digest"] ?: return response.sendBadRequest()
        val digest = try {
            digestParameter.toOciDigest()
        } catch (e: IllegalArgumentException) {
            return response.sendBadRequest()
        }
        val requestHeaders = request.requestHeaders()
//        val contentLengthHeader = requestHeaders[CONTENT_LENGTH]
        val contentRange = try {
            requestHeaders[CONTENT_RANGE]?.decodeRange()
        } catch (e: IllegalArgumentException) {
            return response.sendBadRequest()
        }
        val contentType = requestHeaders[CONTENT_TYPE]
        if ((contentType != null) && (contentType != APPLICATION_OCTET_STREAM.toString())) {
            return response.sendBadRequest()
        }
        val offset = if (contentRange == null) 0 else {
            val currentSize = storage.getBlobUploadSize(repositoryName, id) ?: return response.sendNotFound()
            if (contentRange.first != currentSize) {
                return response.status(REQUESTED_RANGE_NOT_SATISFIABLE).send()
            }
            currentSize
        }
        return storage.finishBlobUpload(repositoryName, id, request.receive(), offset, digest).flatMap {
            response.sendBlobCreated(repositoryName, digest)
        }.onErrorResume { error -> // TODO error handling after flatMap not 100% correct, would also handle errors from the flattened mono
            when (error) {
                is NoSuchElementException -> response.sendNotFound()
                is DigestException -> response.sendBadRequest()
                else -> throw error
            }
        }
    }

    private fun HttpServerResponse.sendBlobCreated(repositoryName: String, digest: OciDigest) =
        status(CREATED).header(LOCATION, "/v2/$repositoryName/blobs/$digest").send()

    private fun deleteBlobUpload(repositoryName: String, id: String, response: HttpServerResponse): Publisher<Void> {
        return response.status(METHOD_NOT_ALLOWED).send()
    }
}

private val URI.queryParameters: Map<String, String> // TODO move to UriExtensions
    get() {
        val query = query ?: return emptyMap()
        return query.split('&').associate {
            Pair(it.substringBefore('='), it.substringAfter('=', ""))
        }
    }

private fun String.decodeRange(): HttpRange { // TODO placement
    val parts = split('-')
    if (parts.size != 2) {
        throw IllegalArgumentException("\"$this\" is not a valid range, it must contain exactly 1 '-' character.")
    }
    val first = parts[0].toLong()
    val last = parts[0].toLong()
    if (last < first) {
        throw IllegalArgumentException("\"$this\" is not a valid range, last position must not be less than first position.")
    }
    return HttpRange(first, last)
}
