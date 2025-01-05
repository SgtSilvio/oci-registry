package io.github.sgtsilvio.oci.registry

import java.nio.file.FileAlreadyExistsException
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import kotlin.io.path.*

/**
 * @author Silvio Giebl
 */
class DistributionRegistryStorage(private val directory: Path) : OciRegistryStorage() {

    override fun getManifest(repositoryName: String, tag: String): ByteArray? =
        resolveManifestTagCurrentLinkFile(repositoryName, tag).readLinkedBlob()

    override fun getManifest(repositoryName: String, digest: OciDigest): ByteArray? =
        resolveManifestLinkFile(repositoryName, digest).readLinkedBlob()

    override fun putManifest(repositoryName: String, digest: OciDigest, data: ByteArray) {
        resolveBlobFile(digest).createParentDirectories().writeAtomicallyIfNotExists { it.writeBytes(data) }
        resolveManifestLinkFile(repositoryName, digest).createParentDirectories()
            .writeAtomicallyIfNotExists { it.writeText(digest.toString()) }
    }

    override fun tagManifest(repositoryName: String, digest: OciDigest, tag: String) {
        resolveManifestTagCurrentLinkFile(repositoryName, tag).createParentDirectories()
            .writeAtomically { it.writeText(digest.toString()) }
        resolveManifestTagIndexLinkFile(repositoryName, tag, digest).createParentDirectories()
            .writeAtomicallyIfNotExists { it.writeText(digest.toString()) }
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

    private fun Path.readLinkedBlob(): ByteArray? {
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

    private fun Path.createParentDirectories(): Path {
        parent?.createDirectories()
        return this
    }

    private inline fun Path.writeAtomically(writeOperation: (Path) -> Unit) {
        val tempFile = createTempFile(parent, name)
        try {
            writeOperation(tempFile)
            tempFile.moveTo(this, true)
        } finally {
            tempFile.deleteIfExists()
        }
    }

    private inline fun Path.writeAtomicallyIfNotExists(writeOperation: (Path) -> Unit) {
        if (exists()) return
        val tempFile = createTempFile(parent, name)
        try {
            writeOperation(tempFile)
            try {
                tempFile.moveTo(this)
            } catch (ignored: FileAlreadyExistsException) {
            }
        } finally {
            tempFile.deleteIfExists()
        }
    }
}
