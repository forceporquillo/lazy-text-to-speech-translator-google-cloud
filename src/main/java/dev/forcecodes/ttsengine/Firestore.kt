package dev.forcecodes.ttsengine

import com.google.cloud.firestore.Firestore
import com.google.cloud.firestore.SetOptions
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Bucket
import com.google.firebase.FirebaseApp
import com.google.firebase.cloud.StorageClient
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.Path
import kotlin.system.exitProcess

private val logger = createLogger("dev.forcecodes.ttsengine.FirestoreKt")

internal fun Firestore.downloadTranslatedFiles(
  app: FirebaseApp,
  path: String,
  executor: ExecutorService,
  downloadPath: String,
  bucket: Bucket = StorageClient.getInstance(app).bucket(),
) = Runnable {

  // ensure folder exist
  File(downloadPath).mkdir()

  var removeListener = false

  val listenerRegistration = translationCollection(path)
    .addSnapshotListener { querySnapshot, exception ->
      if (exception != null) {
        logger.info("Failed to download translated text-to-speech. Error occurred: $exception")
        exitProcess(1)
      }
      querySnapshot?.let { snapshot ->
        val items = snapshot.map {
          Pair(it.id, it.get("audioPath") as? String)
        }
          // ensure only downloadable doc with gs bucket included
          .filterNot { it.second == null }

        items.map {
          // convert uri to blob id
          Pair(it.first, BlobId.fromGsUtilUri(it.second))
        }.forEachIndexed { index, pair ->
          executor.execute {
            logger.info("Downloading translated text-to-speech ${pair.first}")
            // fetch audio link from Google Cloud Storage
            bucket.storage.get(pair.second).downloadTo(Path(downloadPath, "${pair.first}.mp3"))
            if (index == items.lastIndex) {
              executor.shutdown()
            }
          }
        }
      }
    }

  while (executor.isShutdown) {
    listenerRegistration.remove()
    break
  }
}

internal fun Firestore.translateToCollection(
  path: String,
  translation: VoiceTypeTranslation,
  executor: Executor,
  source: () -> List<Speech>,
  transform: (Speech) -> String
) = Runnable {
  var size: Int
  val invokeCount = AtomicInteger(0)
  val collection = translationCollection(path)
  val latch = CountDownLatch(1)
  source()
    .also { size = it.size }
    .map { Pair(it.toString(), transform(it)) }
    .forEach { pair ->
      logger.info("Uploading text-to-speech document: ${pair.first}")
      collection
        .document(pair.first)
        .set(translation.type.translate(pair.second), SetOptions.merge())
        .addListener({
           try {
             logger.info("Successfully uploaded ${translation.type} document with textToTranslate: ${pair.second} to Firestore document!")
           } catch (e: Exception) {
             logger.error("Error encountered while setting document to Firestore. document: ${translation.type}, textToTranslate: ${pair.second}")
           } finally {
             if (size == invokeCount.incrementAndGet()) {
               latch.countDown()
             }
           }
        }, executor)
    }
  latch.await()
}

private fun Firestore.translationCollection(path: String) = collection("${path}_translations")
