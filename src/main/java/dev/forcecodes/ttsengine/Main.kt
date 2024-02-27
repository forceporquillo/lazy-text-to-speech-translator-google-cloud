@file:Suppress("unused")

package dev.forcecodes.ttsengine

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.firestore.Firestore
import com.google.cloud.firestore.SetOptions
import com.google.cloud.storage.BlobId
import com.google.common.util.concurrent.MoreExecutors
import com.google.firebase.FirebaseApp
import com.google.firebase.FirebaseOptions
import com.google.firebase.cloud.FirestoreClient
import com.google.firebase.cloud.StorageClient
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.File
import java.io.FileInputStream
import java.nio.file.Files
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.io.path.Path
import kotlin.system.exitProcess

fun main(args: Array<String>) {

  val tokenPath = args[0]
  val bucketUrl = args[1]
  val excelPathToParse = args[2]
  val pathToCollection = args[3]

  val refreshToken = FileInputStream(tokenPath)
  val options = FirebaseOptions.builder()
    .setCredentials(GoogleCredentials.fromStream(refreshToken))
    .setStorageBucket(bucketUrl)
    .build()

  val app = FirebaseApp.initializeApp(options)
  val firestore = FirestoreClient.getFirestore(app)

  val threadCount = Runtime.getRuntime().availableProcessors() * 2
  val executor = Executors.newWorkStealingPool(threadCount)

  // 1. push text to translate to Firestore collection
  firestore.translateToCollection(
    path = pathToCollection,
    translation = VoiceTypeTranslation.FilipinoTranslation,
    uploadExecutor = executor,
    transform = { it.translation },
    source = { excelWorkSheetParser(excelPathToParse) }
  )
  // 2. Download converted text-to-speech from Google Cloud Storage
  firestore.downloadTranslatedFiles(
    app = app,
    path = pathToCollection,
    downloadPath = "downloaded/$pathToCollection",
    downloadExecutor = executor
  )

  // force main thread to become deadlock
  Thread.currentThread().join()
}

private fun Firestore.downloadTranslatedFiles(
  app: FirebaseApp,
  path: String,
  downloadPath: String,
  downloadExecutor: Executor = MoreExecutors.directExecutor(),
  bucket: com.google.cloud.storage.Bucket = StorageClient.getInstance(app).bucket(),
) {

  // ensure folder exist
  File(downloadPath).mkdir()

  var exit = false

  val executor = Executors.newSingleThreadScheduledExecutor()

  val listenerRegistration =  collection("${path}_translations")
    .addSnapshotListener { querySnapshot, exception ->
      if (exception != null) {
        println("Failed to download translated text-to-speech. Error occurred: $exception")
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
          downloadExecutor.execute {
            println("[$currentThread] Downloading translated text-to-speech ${pair.first}")
            // fetch audio link from Google Cloud Storage
            bucket.storage.get(pair.second)
              .downloadTo(Path(downloadPath, "${pair.first}.mp3"))
            exit = index == items.lastIndex
          }
        }
      }
    }

  executor.scheduleAtFixedRate({
    if (exit) {
      listenerRegistration.remove()
      exitProcess(1)
    }
  }, 0L, 5L, TimeUnit.SECONDS)
}

private fun Firestore.translateToCollection(
  path: String,
  translation: VoiceTypeTranslation,
  uploadExecutor: Executor = MoreExecutors.directExecutor(),
  source: () -> List<Speech>,
  transform: (Speech) -> String
) {
  source()
    .map { Pair(it.toString(), transform(it)) }
    .forEach {
      uploadExecutor.execute {
        println("[$currentThread] Uploading text-to-speech document: ${it.first}")
        collection("${path}_translations")
          .document(it.first)
          .set(translation.type.translate(it.second), SetOptions.merge())
          .get()
      }
    }
}

// Filipino (Philippines)	Neural2	fil-PH	fil-ph-Neural2-A	FEMALE
// English  (US)	        WaveNet	en-US	en-US-Wavenet-H	    FEMALE

sealed class VoiceTypeTranslation(val type: VoiceType) {
  data object FilipinoTranslation : VoiceTypeTranslation(
    VoiceType(
      "fil-PH",
      "fil-ph-Neural2-A",
      "FEMALE")
  )

  data object EnglishTranslation : VoiceTypeTranslation(
    VoiceType(
      "en-US",
      "en-US-Wavenet-H",
      "FEMALE")
  )
}

data class VoiceType(
  val languageCode: String,
  val voiceName: String,
  val ssmlGender: String,
  val audioEncoding: String = "MP3"
) {
  fun translate(textToTranslate: String?): Map<String, String?> {
    return mapOf(
      "text" to textToTranslate,
      "languageCode" to languageCode,     // Optional if per-document overrides are enabled
      "ssmlGender" to ssmlGender,         // Optional if per-document overrides are enabled
      "audioEncoding" to audioEncoding,   // Optional if per-document overrides are enabled
      "voiceName" to voiceName)           // Optional if per-document overrides are enabled
  }
}

fun excelWorkSheetParser(excelPath: String): MutableList<Speech> {
  val sheets = mutableListOf<Speech>()
  try {
    val file = Files.newInputStream(Path(excelPath))
    val workbook = XSSFWorkbook(file)
    val sheet = workbook.getSheet(SHEET)
    val rowIterator = sheet.rowIterator()
    while (rowIterator.hasNext()) {
      val speech = Speech()
      val row = rowIterator.next()
      val cellIterator = row.cellIterator()
      var index = 0
      var position = 0
      while (cellIterator.hasNext()) {
        if (position++ < 2 /* skip header */) {
          continue
        }
        val cell = cellIterator.next()
        if (cell.cellType == CellType.NUMERIC) {
          continue
        }
        val value = cell.stringCellValue
        when (index++) {
          0 -> speech.english = value
          1 -> speech.tagalog = value
          2 -> speech.translation = value
        }
      }
      if (speech.english.isEmpty() || speech.tagalog.isEmpty() || speech.translation.isEmpty()) {
        continue
      }
      sheets.add(speech)
    }
    file.close()
    return sheets
  } catch (e: Exception) {
    e.printStackTrace()
  }
  return mutableListOf()
}

private val currentThread: String
  get() = Thread.currentThread().name

data class Speech(var english: String = "", var tagalog: String = "", var translation: String = "") {
  override fun toString(): String {
    return "${english}_${tagalog}_$translation"
  }
}

const val SHEET = "Ayta Magbukun"

