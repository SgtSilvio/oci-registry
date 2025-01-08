package io.github.sgtsilvio.oci.registry

import reactor.core.publisher.Mono
import reactor.netty.ByteBufFlux
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.*
import java.util.*
import kotlin.NoSuchElementException
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

    override fun mountBlob(repositoryName: String, digest: OciDigest, fromRepositoryName: String): Boolean {
        val blobDigest = try {
            resolveBlobLinkFile(fromRepositoryName, digest).readText()
        } catch (e: IOException) {
            return false
        }.toOciDigest()
        if (!resolveBlobFile(blobDigest).exists()) {
            return false
        }
        resolveBlobLinkFile(repositoryName, digest).createParentDirectories()
            .writeAtomicallyIfNotExists { it.writeText(blobDigest.toString()) }
        return true
    }

    override fun createBlobUpload(repositoryName: String): String {
        val id = UUID.randomUUID().toString()
        resolveBlobUploadDataFile(repositoryName, id).createParentDirectories().createFile()
        return id
    }

    override fun getBlobUploadSize(repositoryName: String, id: String): Long? {
        return try {
            resolveBlobUploadDataFile(repositoryName, id).fileSize()
        } catch (e: IOException) {
            null
        }
    }

    override fun writeBlobUpload(repositoryName: String, id: String, data: ByteBufFlux, offset: Long): Mono<Long> =
        Mono.using(
            {
                try {
                    FileChannel.open(resolveBlobUploadDataFile(repositoryName, id), StandardOpenOption.WRITE)
                } catch (e: IOException) {
                    throw NoSuchElementException()
                }
            },
            { fileChannel ->
                data.scan(offset) { position, byteBuf ->
                    val length = byteBuf.readableBytes()
                    byteBuf.readBytes(fileChannel, position, length)
                    position + length
                }.last()
            },
            { fileChannel -> fileChannel.close() },
        )

    override fun finishBlobUpload(repositoryName: String, id: String, digest: OciDigest): Boolean {
        val blobUploadDataFile = resolveBlobUploadDataFile(repositoryName, id)
        if (!blobUploadDataFile.exists()) return false
        val blobFile = resolveBlobFile(digest).createParentDirectories()
        try {
            if (!blobFile.exists()) {
                try {
                    blobUploadDataFile.moveTo(blobFile)
                } catch (e: IOException) {
                    if ((e is NoSuchFileException) || !blobUploadDataFile.exists()) return false
                    if ((e !is FileAlreadyExistsException) && !blobFile.exists()) throw e
                }
            }
        } finally {
            blobUploadDataFile.deleteIfExists()
            blobUploadDataFile.parent.deleteExisting()
        }
        resolveBlobLinkFile(repositoryName, digest).createParentDirectories()
            .writeAtomicallyIfNotExists { it.writeText(digest.toString()) }
        return true
    }

    private fun Path.getLinkedBlobFile(): Path? {
        val digest = try {
            readText()
        } catch (e: IOException) {
            return null
        }.toOciDigest()
        val blobFile = resolveBlobFile(digest)
        if (!blobFile.exists()) { // TODO
            return null
        }
        return blobFile
    }

    private fun Path.readLinkedBlob(): ByteArray? {
        val digest = try {
            readText()
        } catch (e: IOException) {
            return null
        }.toOciDigest()
        return try {
            resolveBlobFile(digest).readBytes()
        } catch (e: IOException) {
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

    private fun resolveBlobUploadDataFile(repositoryName: String, id: String): Path =
        resolveRepositoryDirectory(repositoryName).resolve("_uploads").resolve(id).resolve("data")

    private fun Path.createParentDirectories(): Path { // TODO move to PathExtensions?
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
            } catch (e: IOException) {
                if ((e !is FileAlreadyExistsException) && !exists()) throw e
            }
        } finally {
            tempFile.deleteIfExists()
        }
    }
}
