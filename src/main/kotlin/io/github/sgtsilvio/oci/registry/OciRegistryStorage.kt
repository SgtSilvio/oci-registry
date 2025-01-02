package io.github.sgtsilvio.oci.registry

import java.nio.file.Path

/**
 * @author Silvio Giebl
 */
sealed class OciRegistryStorage {

    internal abstract fun getManifest(repositoryName: String, tag: String): Path?

    internal abstract fun getManifest(repositoryName: String, digest: OciDigest): Path?

    internal abstract fun putManifest(repositoryName: String, digest: OciDigest, data: ByteArray)

    internal abstract fun tagManifest(repositoryName: String, digest: OciDigest, tag: String)

    internal abstract fun getBlob(repositoryName: String, digest: OciDigest): Path?
}
