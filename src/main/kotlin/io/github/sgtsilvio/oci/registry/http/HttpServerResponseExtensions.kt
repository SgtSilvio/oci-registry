package io.github.sgtsilvio.oci.registry.http

import io.netty.handler.codec.http.HttpHeaderNames.CONTENT_RANGE
import io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST
import io.netty.handler.codec.http.HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE
import reactor.core.publisher.Mono
import reactor.netty.http.server.HttpServerResponse

internal fun HttpServerResponse.sendBadRequest(): Mono<Void> = status(BAD_REQUEST).send()

internal fun HttpServerResponse.sendRangeNotSatisfiable(size: Long): Mono<Void> =
    status(REQUESTED_RANGE_NOT_SATISFIABLE).header(CONTENT_RANGE, "bytes */$size").send()
