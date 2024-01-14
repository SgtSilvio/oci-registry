package io.github.sgtsilvio.oci.registry

import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.readBytes

/**
 * @author Silvio Giebl
 */
class DistributionRegistryStorage(private val directory: Path) : OciRegistryStorage() {

    override fun getManifest(name: String, tag: String): Path? {
        val linkFile = resolveRepositoryDirectory(name).resolve("_manifests/tags").resolve(tag).resolve("current/link")
        return getLinkedBlob(linkFile)
    }

    override fun getManifest(name: String, digest: OciDigest): Path? {
        val linkFile = resolveRepositoryDirectory(name).resolve("_manifests/revisions").resolveLinkFile(digest)
        return getLinkedBlob(linkFile)
    }

    override fun getBlob(name: String, digest: OciDigest): Path? {
        val linkFile = resolveRepositoryDirectory(name).resolve("_layers").resolveLinkFile(digest)
        return getLinkedBlob(linkFile)
    }

    private fun getLinkedBlob(linkFile: Path): Path? {
        if (!linkFile.exists()) {
            return null
        }
        val digest = linkFile.readBytes().decodeToString().toOciDigest()
        val blobFile = directory.resolve("blobs")
            .resolve(digest.algorithm)
            .resolve(digest.hash.substring(0, 2))
            .resolve(digest.hash)
            .resolve("data")
        if (!blobFile.exists()) {
            return null
        }
        return blobFile
    }

    private fun resolveRepositoryDirectory(name: String) = directory.resolve("repositories").resolve(name)

    private fun Path.resolveLinkFile(digest: OciDigest) = resolve(digest.algorithm).resolve(digest.hash).resolve("link")
}
