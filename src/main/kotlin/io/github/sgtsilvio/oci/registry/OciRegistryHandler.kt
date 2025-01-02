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
import java.util.function.BiFunction
import kotlin.io.path.fileSize
import kotlin.io.path.readBytes

/*
https://github.com/opencontainers/distribution-spec/blob/main/spec.md

end-1   GET	    /v2/                                                        200     ✔
end-2   GET	    /v2/<name>/blobs/<digest>                                   200/404 ✔
end-2   HEAD    /v2/<name>/blobs/<digest>                                   200/404 ✔
end-10  DELETE  /v2/<name>/blobs/<digest>                                   405     ✔
end-3   GET	    /v2/<name>/manifests/<reference>                            200/404 ✔
end-3   HEAD    /v2/<name>/manifests/<reference>                            200/404 ✔
end-7   PUT     /v2/<name>/manifests/<reference>                            405     ✔
end-9   DELETE  /v2/<name>/manifests/<reference>                            405     ✔
end-4a  POST    /v2/<name>/blobs/uploads/                                   405     ✔
end-4b  POST    /v2/<name>/blobs/uploads/?digest=<digest>                   405     ✔
end-11  POST    /v2/<name>/blobs/uploads/?mount=<digest>&from=<other_name>  405     ✔
end-13  GET     /v2/<name>/blobs/uploads/<reference>                        405     ✔
end-5   PATCH   /v2/<name>/blobs/uploads/<reference>                        405     ✔
        DELETE  /v2/<name>/blobs/uploads/<reference>                        405     ✔
end-6   PUT     /v2/<name>/blobs/uploads/<reference>?digest=<digest>        405     ✔
end-8a  GET     /v2/<name>/tags/list                                        405     ✔
end-8b  GET     /v2/<name>/tags/list?n=<integer>&last=<tag name>            405     ✔
end-12a GET     /v2/<name>/referrers/<digest>                               404
end-12b GET     /v2/<name>/referrers/<digest>?artifactType=<artifactType>   404
        GET     /v2/_catalog                                                405     ✔
        GET     /v2/_catalog?n=<integer>&last=<repository name>             405     ✔
 */

/**
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
        GET -> response.status(METHOD_NOT_ALLOWED).send()
        else -> response.status(METHOD_NOT_ALLOWED).send()
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
        DELETE -> response.status(METHOD_NOT_ALLOWED).send()
        else -> response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun getOrHeadManifest(
        repositoryName: String,
        reference: String,
        isGet: Boolean,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val manifestFile = if (':' in reference) {
            val digest = try {
                reference.toOciDigest()
            } catch (e: IllegalArgumentException) {
                return response.sendBadRequest()
            }
            storage.getManifest(repositoryName, digest)
        } else {
            storage.getManifest(repositoryName, reference)
        } ?: return response.sendNotFound()
        val manifestBytes = manifestFile.readBytes()
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
        val contentTypeHeader = request.requestHeaders()[CONTENT_TYPE]
        return request.receive().aggregate().asByteArray().flatMap { data ->
            putManifest(repositoryName, digest, tag, contentTypeHeader, data, response)
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
        val actualDigest = data.calculateOciDigest(digest?.algorithm ?: OciDigestAlgorithm.SHA_256)
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
        storage.putManifest(repositoryName, actualDigest, data)
        if (tag != null) {
            storage.tagManifest(repositoryName, actualDigest, tag)
        }
        response.header(LOCATION, "/v2/$repositoryName/manifests/${tag ?: actualDigest}")
        return response.status(CREATED).send()
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
            DELETE -> response.status(METHOD_NOT_ALLOWED).send()
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
            GET, HEAD -> getOrHeadBlobUpload(repositoryName, id, request, response)
            PATCH -> patchBlobUpload(repositoryName, id, request, response)
            PUT -> putBlobUpload(repositoryName, id, request, response)
            DELETE -> response.status(METHOD_NOT_ALLOWED).send()
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
        if (request.requestHeaders()[CONTENT_TYPE] != APPLICATION_OCTET_STREAM.toString()) {
            return response.sendBadRequest()
        }
//        request headers:
//            Content-Length: <length>
//        response.status(CREATED)
//        response.header(LOCATION, "/v2/$repositoryName/blobs/$digest")
//        request.receive()
        return response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun mountBlob(
        repositoryName: String,
        rawDigest: String,
        fromRepositoryName: String,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val digest = try { // TODO move to mountBlob?
            rawDigest.toOciDigest()
        } catch (e: IllegalArgumentException) {
            return response.sendBadRequest()
        }
//        response.status(CREATED)
//        response.header(LOCATION, "/v2/$repositoryName/blobs/$digest")
//        fall back to createBlobUpload when blob is not found
        return response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun createBlobUpload(repositoryName: String, response: HttpServerResponse): Publisher<Void> {
//        request headers for chunked upload:
//            Content-Length: 0
//        response.status(ACCEPTED)
//        response.header(LOCATION, "/v2/$repositoryName/blobs/uploads/$id")
        return response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun getOrHeadBlobUpload(
        repositoryName: String,
        id: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> {
//        response.status(NO_CONTENT) // or 404
//        response.header(LOCATION, "/v2/$repositoryName/blobs/uploads/$id")
//        response.header(RANGE, "0-$endOfRange")
        return response.status(METHOD_NOT_ALLOWED).send()
    }

    private fun patchBlobUpload(
        repositoryName: String,
        id: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> {
        if (request.requestHeaders()[CONTENT_TYPE] != APPLICATION_OCTET_STREAM.toString()) {
            return response.sendBadRequest()
        }
        val contentRangeHeader = request.requestHeaders()[CONTENT_RANGE] ?: return response.sendBadRequest()
//        request headers:
//            Content-Length: <length of chunk>
//        response.status(ACCEPTED) // or 416 range not satisfiable, or 404
//        response.header(LOCATION, "/v2/$repositoryName/blobs/uploads/$id")
//        response.header(RANGE, "0-$endOfRange")
//        request.receive()
//        Chunks MUST be uploaded in order, with the first byte of a chunk being the last chunk's <end-of-range> plus one.
//        If a chunk is uploaded out of order, the registry MUST respond with a 416 Requested Range Not Satisfiable code.
//        A GET request may be used to retrieve the current valid offset and upload location.
        return response.status(METHOD_NOT_ALLOWED).send()
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
        val contentRangeHeader = request.requestHeaders()[CONTENT_RANGE]
        if (contentRangeHeader != null) {
            if (request.requestHeaders()[CONTENT_TYPE] != APPLICATION_OCTET_STREAM.toString()) {
                return response.sendBadRequest()
            }
//            request headers for chunked upload with last chunk:
//                Content-Length: <length of chunk>
        } else {
//            request headers for monolithic upload:
//                Content-Type: application/octet-stream
//                Content-Length: <length>
//            no request headers for chunked upload without last chunk
        }
//        response.status(CREATED) // or 416 range not satisfiable, or 404
//        response.header(LOCATION, "/v2/$repositoryName/blobs/$digest")
//        request.receive()
        return response.status(METHOD_NOT_ALLOWED).send()
    }
}

private val URI.queryParameters get() = query.split('&').associate { // TODO move to UriExtensions
    Pair(it.substringBefore('='), it.substringAfter('=', ""))
}
