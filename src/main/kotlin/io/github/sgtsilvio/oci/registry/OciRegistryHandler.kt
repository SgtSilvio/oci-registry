package io.github.sgtsilvio.oci.registry

import io.netty.handler.codec.http.HttpHeaderNames.*
import io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_OCTET_STREAM
import io.netty.handler.codec.http.HttpMethod.*
import io.netty.handler.codec.http.HttpResponseStatus.*
import org.json.JSONObject
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.server.HttpServerResponse
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
class OciRegistryHandler(private val storage: OciRegistryStorage) :
    BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

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

            "/_catalog" -> return when (request.method()) {
                GET -> response.status(METHOD_NOT_ALLOWED).send()
                else -> response.status(METHOD_NOT_ALLOWED).send()
            }
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
                "list" -> when (request.method()) {
                    GET -> response.status(METHOD_NOT_ALLOWED).send()
                    else -> response.status(METHOD_NOT_ALLOWED).send()
                }

                else -> response.sendNotFound()
            }

            "manifests" -> when (request.method()) {
                GET -> getOrHeadManifest(firstSegments, lastSegment, true, response)
                HEAD -> getOrHeadManifest(firstSegments, lastSegment, false, response)
                PUT, DELETE -> response.status(METHOD_NOT_ALLOWED).send()
                else -> response.status(METHOD_NOT_ALLOWED).send()
            }

            "blobs" -> when (request.method()) {
                GET -> getBlob(firstSegments, lastSegment, request, response)
                HEAD -> headBlob(firstSegments, lastSegment, response)
                DELETE -> response.status(METHOD_NOT_ALLOWED).send()
                else -> response.status(METHOD_NOT_ALLOWED).send()
            }

            "uploads" -> when {
                firstSegments.endsWith("/blobs") -> when (request.method()) {
                    POST, GET, PATCH, PUT, DELETE -> response.status(METHOD_NOT_ALLOWED).send()
                    else -> response.status(METHOD_NOT_ALLOWED).send()
                }

                else -> response.sendNotFound()
            }

            else -> response.sendNotFound()
        }
    }

    private fun getOrHeadManifest(
        name: String,
        reference: String,
        isGET: Boolean,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val manifestFile = if (':' in reference) {
            val digest = try {
                reference.toOciDigest()
            } catch (e: IllegalArgumentException) {
                return response.sendBadRequest()
            }
            storage.getManifest(name, digest)
        } else {
            storage.getManifest(name, reference)
        } ?: return response.sendNotFound()
        val manifestBytes = manifestFile.readBytes()
        response.header(CONTENT_TYPE, JSONObject(manifestBytes.decodeToString()).getString("mediaType"))
        response.header(CONTENT_LENGTH, manifestBytes.size.toString())
        return if (isGET) response.sendByteArray(Mono.just(manifestBytes)) else response.send()
    }

    private fun getBlob(
        name: String,
        rawDigest: String,
        request: HttpServerRequest,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val digest = try {
            rawDigest.toOciDigest()
        } catch (e: IllegalArgumentException) {
            return response.sendBadRequest()
        }
        val blobFile = storage.getBlob(name, digest) ?: return response.sendNotFound()
        val size = blobFile.fileSize()
        val rangeHeader: String? = request.requestHeaders()[RANGE]
        if ((rangeHeader != null) && rangeHeader.startsWith("bytes=") && (',' !in rangeHeader)) {
            val rangeParts = rangeHeader.substring("bytes=".length).split('-')
            if (rangeParts.size != 2) {
                return response.sendBadRequest()
            }
            val rangePart1 = rangeParts[0].trim()
            val rangePart2 = rangeParts[1].trim()
            val start: Long
            val end: Long
            if (rangePart1.isEmpty()) {
                end = size
                start = size - try {
                    rangePart2.toLong()
                } catch (e: NumberFormatException) {
                    return response.sendBadRequest()
                }
                if (start < 0) {
                    return response.sendRangeNotSatisfiable(size)
                }
            } else {
                start = try {
                    rangePart1.toLong()
                } catch (e: NumberFormatException) {
                    return response.sendBadRequest()
                }
                end = if (rangePart2.isEmpty()) size else try {
                    rangePart2.toLong() + 1
                } catch (e: NumberFormatException) {
                    return response.sendBadRequest()
                }
                if (start >= end) {
                    return response.sendBadRequest()
                }
                if ((start >= size) || (end > size)) {
                    return response.sendRangeNotSatisfiable(size)
                }
            }
            val partialSize = end - start
            response.header(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
            response.header(CONTENT_LENGTH, partialSize.toString())
            response.header(CONTENT_RANGE, "bytes $start-${end - 1}/$size")
            return response.status(PARTIAL_CONTENT).sendFile(blobFile, start, partialSize)
        }
        response.header(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
        response.header(CONTENT_LENGTH, size.toString())
        return response.sendFile(blobFile, 0, size)
    }

    private fun headBlob(name: String, rawDigest: String, response: HttpServerResponse): Publisher<Void> {
        val digest = try {
            rawDigest.toOciDigest()
        } catch (e: IllegalArgumentException) {
            return response.sendBadRequest()
        }
        val blobFile = storage.getBlob(name, digest) ?: return response.sendNotFound()
        response.header(CONTENT_TYPE, APPLICATION_OCTET_STREAM)
        response.header(CONTENT_LENGTH, blobFile.fileSize().toString())
        return response.send()
    }

    private fun HttpServerResponse.sendBadRequest(): Mono<Void> = status(BAD_REQUEST).send()

    private fun HttpServerResponse.sendRangeNotSatisfiable(size: Long): Mono<Void> =
        status(REQUESTED_RANGE_NOT_SATISFIABLE).header(CONTENT_RANGE, "bytes */$size").send()
}
