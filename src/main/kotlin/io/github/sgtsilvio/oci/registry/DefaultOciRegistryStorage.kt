package io.github.sgtsilvio.oci.registry

import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.readBytes

/**
 * @author Silvio Giebl
 */
class DefaultOciRegistryStorage(private val directory: Path) : OciRegistryStorage() {

    override fun getManifest(name: String, tag: String): Path? {
        val linkFile = resolveRepositoryDirectory(name).resolve("manifests").resolve(tag)
        return getLinkedBlob(linkFile)
    }

    override fun getManifest(name: String, digest: OciDigest): Path? {
        val linkFile = resolveRepositoryDirectory(name).resolve("manifests").resolveDigestFile(digest)
        return getLinkedBlob(linkFile)
    }

    override fun getBlob(name: String, digest: OciDigest): Path? {
        val linkFile = resolveRepositoryDirectory(name).resolve("blobs").resolveDigestFile(digest)
        return getLinkedBlob(linkFile)
    }

    private fun getLinkedBlob(linkFile: Path): Path? {
        if (!linkFile.exists()) {
            return null
        }
        val digest = linkFile.readBytes().decodeToString().toOciDigest()
        val blobFile = directory.resolve("blobs").resolveDigestFile(digest)
        if (!blobFile.exists()) {
            return null
        }
        return blobFile
    }

    private fun resolveRepositoryDirectory(name: String): Path = directory.resolve("repositories").resolve(name)

    private fun Path.resolveDigestFile(digest: OciDigest): Path = resolve(digest.algorithm).resolve(digest.hash)
}
