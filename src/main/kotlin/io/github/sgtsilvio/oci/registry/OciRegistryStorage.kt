package io.github.sgtsilvio.oci.registry

import io.netty.buffer.ByteBuf
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.nio.file.Path

/**
 * @author Silvio Giebl
 */
sealed class OciRegistryStorage {

    internal abstract fun getTags(repositoryName: OciRepositoryName): List<OciTag>?

    internal abstract fun getManifest(
        repositoryName: OciRepositoryName,
        tagOrDigest: OciTagOrDigest,
    ): Pair<OciDigest, ByteArray>?

    internal abstract fun putManifest(repositoryName: OciRepositoryName, digest: OciDigest, data: ByteArray)

    internal abstract fun tagManifest(repositoryName: OciRepositoryName, digest: OciDigest, tag: OciTag)

    internal abstract fun getBlob(repositoryName: OciRepositoryName, digest: OciDigest): Path? // TODO return Flux<ByteArray> or ByteBufFlux, error if not found?

    internal abstract fun mountBlob(
        repositoryName: OciRepositoryName,
        digest: OciDigest,
        fromRepositoryName: OciRepositoryName?,
    ): Boolean

    internal abstract fun createBlobUpload(repositoryName: OciRepositoryName): String

    internal abstract fun getBlobUploadSize(repositoryName: OciRepositoryName, id: String): Long?

    internal abstract fun progressBlobUpload(
        repositoryName: OciRepositoryName,
        id: String,
        data: Flux<ByteBuf>,
        offset: Long,
    ): Mono<Long>

    internal abstract fun finishBlobUpload(
        repositoryName: OciRepositoryName,
        id: String,
        data: Flux<ByteBuf>,
        offset: Long,
        digest: OciDigest,
    ): Mono<OciDigest>
}
