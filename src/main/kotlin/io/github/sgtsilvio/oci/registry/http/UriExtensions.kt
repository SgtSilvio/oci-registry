package io.github.sgtsilvio.oci.registry.http

import java.net.URI
import java.net.URLDecoder

internal val URI.queryParameters: Map<String, List<String>>
    get() = rawQuery?.split('&')
        ?.groupBy({ it.substringBefore('=').formUrlDecode() }, { it.substringAfter('=', "").formUrlDecode() })
        ?: emptyMap()

private fun String.formUrlDecode(): String = URLDecoder.decode(this, "UTF-8")
