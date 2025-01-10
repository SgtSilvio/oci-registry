package io.github.sgtsilvio.oci.registry

import io.netty.buffer.ByteBuf
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.security.DigestException
import java.security.MessageDigest
import java.util.*
import kotlin.io.path.*
import kotlin.math.min

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

    override fun getBlob(repositoryName: String, digest: OciDigest): Path? {
        val blobFile = resolveBlobLinkFile(repositoryName, digest).resolveLinkedBlobFile() ?: return null
        if (!blobFile.exists()) { // TODO
            return null
        }
        return blobFile
    }

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

    override fun progressBlobUpload(repositoryName: String, id: String, data: Flux<ByteBuf>, offset: Long): Mono<Long> =
        Mono.using(
            {
                try {
                    FileChannel.open(resolveBlobUploadDataFile(repositoryName, id), StandardOpenOption.WRITE)
                } catch (e: IOException) {
                    throw NoSuchElementException()
                }
            },
            { fileChannel -> fileChannel.write(data, offset) },
        )

    override fun finishBlobUpload(
        repositoryName: String,
        id: String,
        data: Flux<ByteBuf>,
        offset: Long,
        digest: OciDigest
    ): Mono<OciDigest> {
        val blobUploadDataFile = resolveBlobUploadDataFile(repositoryName, id)
        return Mono.using(
            {
                try {
                    FileChannel.open(blobUploadDataFile, StandardOpenOption.READ, StandardOpenOption.WRITE)
                } catch (e: IOException) {
                    throw NoSuchElementException()
                }
            },
            { fileChannel ->
                val messageDigest = digest.algorithm.createMessageDigest()
                if (offset > 0) {
                    messageDigest.update(fileChannel, 0, offset)
                }
                fileChannel.write(
                    data.doOnNext { byteBuf -> messageDigest.update(byteBuf.nioBuffer()) },
                    offset,
                ).map { position ->
                    if (position < fileChannel.size()) {
                        messageDigest.update(fileChannel, position, Long.MAX_VALUE)
                    }
                    OciDigest(digest.algorithm, messageDigest.digest())
                }
            },
        ).doOnNext { actualDigest ->
            try {
                if (digest != actualDigest) {
                    throw DigestException()
                }
                val blobFile = resolveBlobFile(digest).createParentDirectories()
                blobUploadDataFile.moveToIfNotExists(blobFile)
            } finally {
                blobUploadDataFile.deleteIfExists()
                blobUploadDataFile.parent.deleteIfExists()
            }
            resolveBlobLinkFile(repositoryName, digest).createParentDirectories()
                .writeAtomicallyIfNotExists { it.writeText(digest.toString()) }
        }
    }

    private fun FileChannel.write(data: Flux<ByteBuf>, offset: Long): Mono<Long> =
        data.scan(offset) { position, byteBuf ->
            val length = byteBuf.readableBytes()
            byteBuf.readBytes(this, position, length)
            position + length
        }.last()

    private fun MessageDigest.update(fileChannel: FileChannel, offset: Long, length: Long) {
        val bufferCapacity = 8192
        val buffer = ByteBuffer.allocate(bufferCapacity)
        var position = offset
        var remaining = length
        while (remaining > 0) {
            buffer.position(0)
            buffer.limit(min(remaining, bufferCapacity.toLong()).toInt())
            val read = fileChannel.read(buffer, position)
            if (read == -1) {
                if (length == Long.MAX_VALUE) {
                    break
                }
                throw IllegalStateException() // TODO
            }
            buffer.flip()
            update(buffer)
            position += read
            remaining -= read
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

    private fun Path.resolveLinkedBlobFile(): Path? {
        val digest = try {
            readText()
        } catch (e: IOException) {
            return null
        }.toOciDigest()
        return resolveBlobFile(digest)
    }

    private fun Path.readLinkedBlob(): ByteArray? {
        val blobFile = resolveLinkedBlobFile() ?: return null
        return try {
            blobFile.readBytes()
        } catch (e: IOException) {
            null
        }
    }

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
            tempFile.moveToIfNotExists(this)
        } finally {
            tempFile.deleteIfExists()
        }
    }

    private fun Path.moveToIfNotExists(target: Path) {
        try {
            moveTo(target)
        } catch (e: IOException) {
            if ((e !is FileAlreadyExistsException) && !target.exists()) throw e
        }
    }
}
