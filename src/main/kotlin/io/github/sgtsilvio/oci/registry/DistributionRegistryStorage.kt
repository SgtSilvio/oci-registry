package io.github.sgtsilvio.oci.registry

import java.nio.file.NoSuchFileException
import java.nio.file.Path
import kotlin.io.path.*

/**
 * @author Silvio Giebl
 */
class DistributionRegistryStorage(private val directory: Path) : OciRegistryStorage() {

    override fun getManifest(repositoryName: String, tag: String): ByteArray? =
        resolveManifestTagCurrentLinkFile(repositoryName, tag).getLinkedBlob()

    override fun getManifest(repositoryName: String, digest: OciDigest): ByteArray? =
        resolveManifestLinkFile(repositoryName, digest).getLinkedBlob()

    override fun putManifest(repositoryName: String, digest: OciDigest, data: ByteArray) {
        val blobFile = resolveBlobFile(digest)
        if (!blobFile.exists()) {
            blobFile.createParentDirectories().writeBytes(data) // TODO write to tmp file and move for atomicity
        }
        val linkFile = resolveManifestLinkFile(repositoryName, digest)
        if (!linkFile.exists()) {
            linkFile.createParentDirectories().writeText(digest.toString()) // TODO write to tmp file and move for atomicity
        }
    }

    override fun tagManifest(repositoryName: String, digest: OciDigest, tag: String) {
        val currentLinkFile = resolveManifestTagCurrentLinkFile(repositoryName, tag)
        currentLinkFile.createParentDirectories().writeText(digest.toString()) // TODO write to tmp file and move for atomicity
        val indexLinkFile = resolveManifestTagIndexLinkFile(repositoryName, tag, digest)
        if (!indexLinkFile.exists()) {
            indexLinkFile.createParentDirectories().writeText(digest.toString()) // TODO write to tmp file and move for atomicity
        }
    }

    override fun getBlob(repositoryName: String, digest: OciDigest): Path? =
        resolveBlobLinkFile(repositoryName, digest).getLinkedBlobFile()

    private fun Path.getLinkedBlobFile(): Path? {
        val digest = try {
            readText()
        } catch (e: NoSuchFileException) {
            return null
        }.toOciDigest()
        val blobFile = resolveBlobFile(digest)
        if (!blobFile.exists()) {
            return null
        }
        return blobFile
    }

    private fun Path.getLinkedBlob(): ByteArray? {
        val digest = try {
            readText()
        } catch (e: NoSuchFileException) {
            return null
        }.toOciDigest()
        val blobFile = resolveBlobFile(digest)
        return try {
            blobFile.readBytes()
        } catch (e: NoSuchFileException) {
            null
        }
    }

    private fun resolveBlobFile(digest: OciDigest): Path {
        val encodedHash = digest.encodedHash
        return directory.resolve("blobs")
            .resolve(digest.algorithm.id)
            .resolve(encodedHash.substring(0, 2))
            .resolve(encodedHash)
            .resolve("data")
    }

    private fun resolveRepositoryDirectory(repositoryName: String): Path =
        directory.resolve("repositories").resolve(repositoryName)

    private fun resolveManifestLinkFile(repositoryName: String, digest: OciDigest): Path =
        resolveRepositoryDirectory(repositoryName).resolve("_manifests/revisions").resolveLinkFile(digest)

    private fun resolveManifestTagDirectory(repositoryName: String, tag: String): Path =
        resolveRepositoryDirectory(repositoryName).resolve("_manifests/tags").resolve(tag)

    private fun resolveManifestTagCurrentLinkFile(repositoryName: String, tag: String): Path =
        resolveManifestTagDirectory(repositoryName, tag).resolve("current/link")

    private fun resolveManifestTagIndexLinkFile(repositoryName: String, tag: String, digest: OciDigest): Path =
        resolveManifestTagDirectory(repositoryName, tag).resolve("index").resolveLinkFile(digest)

    private fun resolveBlobLinkFile(repositoryName: String, digest: OciDigest): Path =
        resolveRepositoryDirectory(repositoryName).resolve("_layers").resolveLinkFile(digest)

    private fun Path.resolveLinkFile(digest: OciDigest): Path =
        resolve(digest.algorithm.id).resolve(digest.encodedHash).resolve("link")
}
