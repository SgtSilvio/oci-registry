package io.github.sgtsilvio.oci.registry

import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpMethod.*
import org.json.JSONObject
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono
import reactor.netty.http.server.HttpServerRequest
import reactor.netty.http.server.HttpServerResponse
import java.nio.file.Files
import java.nio.file.Path
import java.util.function.BiFunction

/*
https://github.com/opencontainers/distribution-spec/blob/main/spec.md
https://docs.docker.com/registry/spec/api/

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
end-12a GET     /v2/<name>/referrers/<digest>
end-12b GET     /v2/<name>/referrers/<digest>?artifactType=<artifactType>
        GET     /v2/_catalog                                                405     ✔
        GET     /v2/_catalog?n=<integer>&last=<repository name>             405     ✔
 */

/**
 * @author Silvio Giebl
 */
class OciRegistryHandler(private val directory: Path) :
    BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {

    override fun apply(request: HttpServerRequest, response: HttpServerResponse): Publisher<Void> {
        val segments = request.fullPath().substring(1).split('/')
        return when {
            segments[0] == "v2" -> handleV2(request, segments.drop(1), response)
            else -> response.sendNotFound()
        }
    }

    private fun handleV2(
        request: HttpServerRequest,
        segments: List<String>,
        response: HttpServerResponse,
    ): Publisher<Void> = when {
        segments.isEmpty() || segments[0].isEmpty() -> when (request.method()) {
            GET, HEAD -> response.header("Docker-Distribution-API-Version", "registry/2.0").send()
            else -> response.status(405).send()
        }

        (segments.size == 1) && (segments[0] == "_catalog") -> when (request.method()) {
            GET -> response.status(405).send()
            else -> response.status(405).send()
        }

        segments.size < 3 -> response.sendNotFound()

        else -> when (segments[segments.lastIndex - 1]) {
            "tags" -> if (segments[segments.lastIndex] == "list") {
                when (request.method()) {
                    GET -> response.status(405).send()
                    else -> response.status(405).send()
                }
            } else response.sendNotFound()

            "manifests" -> when (request.method()) {
                GET -> getOrHeadManifest(segments, true, response)
                HEAD -> getOrHeadManifest(segments, false, response)
                PUT, DELETE -> response.status(405).send()
                else -> response.status(405).send()
            }

            "blobs" -> when (request.method()) {
                GET -> getOrHeadBlob(segments, true, response)
                HEAD -> getOrHeadBlob(segments, false, response)
                DELETE -> response.status(405).send()
                else -> response.status(405).send()
            }

            "uploads" -> when (request.method()) {
                POST, GET, PATCH, PUT, DELETE -> response.status(405).send()
                else -> response.status(405).send()
            }

            else -> response.sendNotFound()
        }
    }

    private fun getOrHeadManifest(
        segments: List<String>,
        isGET: Boolean,
        response: HttpServerResponse,
    ): Publisher<Void> {
        val name = decodeName(segments, segments.lastIndex - 1)
        val reference = segments[segments.lastIndex]
        val manifestsDirectory = resolveRepositoryDirectory(name).resolve("_manifests")
        val digest = if (':' in reference) {
            val digest = reference.toOciDigest()
            if (!Files.exists(manifestsDirectory.resolve("revisions").resolveLinkFile(digest))) {
                return response.sendNotFound()
            }
            digest
        } else {
            val linkFile = manifestsDirectory.resolve("tags").resolve(reference).resolve("current/link")
            if (!Files.exists(linkFile)) {
                return response.sendNotFound()
            }
            Files.readAllBytes(linkFile).decodeToString().toOciDigest()
        }
        val dataFile = resolveBlobFile(digest)
        if (!Files.exists(dataFile)) {
            return response.sendNotFound()
        }
        val data = Files.readAllBytes(dataFile)
        response.header(HttpHeaderNames.CONTENT_TYPE, JSONObject(data.decodeToString()).getString("mediaType"))
        response.header(HttpHeaderNames.CONTENT_LENGTH, data.size.toString())
        return if (isGET) response.sendByteArray(Mono.just(data)) else response.send()
    }

    private fun getOrHeadBlob(segments: List<String>, isGET: Boolean, response: HttpServerResponse): Publisher<Void> {
        val name = decodeName(segments, segments.lastIndex - 1)
        val digest = segments[segments.lastIndex].toOciDigest()
        if (!Files.exists(resolveRepositoryDirectory(name).resolve("_layers").resolveLinkFile(digest))) {
            return response.sendNotFound()
        }
        val dataFile = resolveBlobFile(digest)
        if (!Files.exists(dataFile)) {
            return response.sendNotFound()
        }
        response.header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_OCTET_STREAM)
        response.header(HttpHeaderNames.CONTENT_LENGTH, Files.size(dataFile).toString())
        return if (isGET) response.sendFile(dataFile) else response.send()
    }

    private fun decodeName(segments: List<String>, toIndex: Int) = segments.subList(0, toIndex).joinToString("/")

    private fun resolveRepositoryDirectory(name: String) = directory.resolve("repositories").resolve(name)

    private fun Path.resolveLinkFile(digest: OciDigest) = resolve(digest.algorithm).resolve(digest.hash).resolve("link")

    private fun resolveBlobFile(digest: OciDigest) =
        directory.resolve("blobs").resolve(digest.algorithm).resolve(digest.hash.substring(0, 2)).resolve(digest.hash)
            .resolve("data")
}
