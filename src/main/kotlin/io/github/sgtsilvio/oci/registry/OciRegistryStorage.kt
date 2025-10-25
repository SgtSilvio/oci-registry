package io.github.sgtsilvio.oci.registry

import io.netty.buffer.ByteBuf
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.nio.file.Path

/**
 * @author Silvio Giebl
 */
sealed class OciRegistryStorage {

    internal abstract fun getManifest(repositoryName: String, reference: OciReference): Pair<OciDigest, ByteArray>?

    internal abstract fun putManifest(repositoryName: String, digest: OciDigest, data: ByteArray)

    internal abstract fun tagManifest(repositoryName: String, digest: OciDigest, tag: OciTag)

    internal abstract fun getBlob(repositoryName: String, digest: OciDigest): Path? // TODO return Flux<ByteArray> or ByteBufFlux, error if not found?

    internal abstract fun mountBlob(repositoryName: String, digest: OciDigest, fromRepositoryName: String): Boolean

    internal abstract fun createBlobUpload(repositoryName: String): String

    internal abstract fun getBlobUploadSize(repositoryName: String, id: String): Long?

    internal abstract fun progressBlobUpload(
        repositoryName: String,
        id: String,
        data: Flux<ByteBuf>,
        offset: Long,
    ): Mono<Long>

    internal abstract fun finishBlobUpload(
        repositoryName: String,
        id: String,
        data: Flux<ByteBuf>,
        offset: Long,
        digest: OciDigest,
    ): Mono<OciDigest>
}
