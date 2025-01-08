package io.github.sgtsilvio.oci.registry

import reactor.core.publisher.Mono
import reactor.netty.ByteBufFlux
import java.nio.file.Path

/**
 * @author Silvio Giebl
 */
sealed class OciRegistryStorage {

    internal abstract fun getManifest(repositoryName: String, tag: String): ByteArray?

    internal abstract fun getManifest(repositoryName: String, digest: OciDigest): ByteArray?

    internal abstract fun putManifest(repositoryName: String, digest: OciDigest, data: ByteArray)

    internal abstract fun tagManifest(repositoryName: String, digest: OciDigest, tag: String)

    internal abstract fun getBlob(repositoryName: String, digest: OciDigest): Path? // TODO return Flux<ByteArray> or ByteBufFlux, error if not found?

    internal abstract fun mountBlob(repositoryName: String, digest: OciDigest, fromRepositoryName: String): Boolean

    internal abstract fun createBlobUpload(repositoryName: String): String

    internal abstract fun getBlobUploadSize(repositoryName: String, id: String): Long?

    internal abstract fun writeBlobUpload(
        repositoryName: String,
        id: String,
        data: ByteBufFlux,
        offset: Long,
    ): Mono<Long>

    internal abstract fun finishBlobUpload(repositoryName: String, id: String, digest: OciDigest): Boolean
}
