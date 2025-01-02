package io.github.sgtsilvio.oci.registry

import java.nio.file.Path

/**
 * @author Silvio Giebl
 */
sealed class OciRegistryStorage {

    internal abstract fun getManifest(name: String, tag: String): Path?

    internal abstract fun getManifest(name: String, digest: OciDigest): Path?

    internal abstract fun putManifest(name: String, digest: OciDigest, data: ByteArray)

    internal abstract fun tagManifest(name: String, digest: OciDigest, tag: String)

    internal abstract fun getBlob(name: String, digest: OciDigest): Path?
}
