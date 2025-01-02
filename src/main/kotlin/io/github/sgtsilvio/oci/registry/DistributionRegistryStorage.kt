package io.github.sgtsilvio.oci.registry

import java.nio.file.Path
import kotlin.io.path.*

/**
 * @author Silvio Giebl
 */
class DistributionRegistryStorage(private val directory: Path) : OciRegistryStorage() {

    override fun getManifest(name: String, tag: String): Path? =
        resolveManifestTagCurrentLinkFile(name, tag).getLinkedBlobFile()

    override fun getManifest(name: String, digest: OciDigest): Path? =
        resolveManifestLinkFile(name, digest).getLinkedBlobFile()

    override fun putManifest(name: String, digest: OciDigest, data: ByteArray) {
        val blobFile = resolveBlobFile(digest)
        if (!blobFile.exists()) {
            blobFile.createParentDirectories().writeBytes(data) // TODO write to tmp file and move for atomicity
        }
        val linkFile = resolveManifestLinkFile(name, digest)
        if (!linkFile.exists()) {
            linkFile.createParentDirectories().writeText(digest.toString()) // TODO write to tmp file and move for atomicity
        }
    }

    override fun tagManifest(name: String, digest: OciDigest, tag: String) {
        val currentLinkFile = resolveManifestTagCurrentLinkFile(name, tag)
        currentLinkFile.createParentDirectories().writeText(digest.toString()) // TODO write to tmp file and move for atomicity
        val indexLinkFile = resolveManifestTagIndexLinkFile(name, tag, digest)
        if (!indexLinkFile.exists()) {
            indexLinkFile.createParentDirectories().writeText(digest.toString()) // TODO write to tmp file and move for atomicity
        }
    }

    override fun getBlob(name: String, digest: OciDigest): Path? = resolveBlobLinkFile(name, digest).getLinkedBlobFile()

    private fun Path.getLinkedBlobFile(): Path? {
        if (!exists()) {
            return null
        }
        val digest = readText().toOciDigest() // TODO handle IOException
        val blobFile = resolveBlobFile(digest)
        if (!blobFile.exists()) {
            return null
        }
        return blobFile
    }

    private fun resolveBlobFile(digest: OciDigest): Path {
        val encodedHash = digest.encodedHash
        return directory.resolve("blobs")
            .resolve(digest.algorithm.id)
            .resolve(encodedHash.substring(0, 2))
            .resolve(encodedHash)
            .resolve("data")
    }

    private fun resolveRepositoryDirectory(name: String): Path = directory.resolve("repositories").resolve(name)

    private fun resolveManifestLinkFile(name: String, digest: OciDigest): Path =
        resolveRepositoryDirectory(name).resolve("_manifests/revisions").resolveLinkFile(digest)

    private fun resolveManifestTagDirectory(name: String, tag: String): Path =
        resolveRepositoryDirectory(name).resolve("_manifests/tags").resolve(tag)

    private fun resolveManifestTagCurrentLinkFile(name: String, tag: String): Path =
        resolveManifestTagDirectory(name, tag).resolve("current/link")

    private fun resolveManifestTagIndexLinkFile(name: String, tag: String, digest: OciDigest): Path =
        resolveManifestTagDirectory(name, tag).resolve("index").resolveLinkFile(digest)

    private fun resolveBlobLinkFile(name: String, digest: OciDigest): Path =
        resolveRepositoryDirectory(name).resolve("_layers").resolveLinkFile(digest)

    private fun Path.resolveLinkFile(digest: OciDigest): Path =
        resolve(digest.algorithm.id).resolve(digest.encodedHash).resolve("link")
}
