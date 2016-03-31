package it.eng.exabeat.test.commons

import java.io.{File, IOException}
import java.util.UUID

/**
  * @author Antonio Murgia 
  * @version 25/03/16
  */
trait TempDirSupport {
  def createTempDir(root: String = getRootDir): File = {
    val dir = createDirectory(root)
    registerShutdownDeleteDir(dir)
    dir
  }

  private def getRootDir(): String = {
    System.getProperty("java.io.tmpdir")
  }

  private val TEMP_DIR_PREFIX = "spark-"
  /**
    * Create a directory inside the given parent directory. The directory is guaranteed to be
    * newly created, and is not marked for automatic deletion.
    */
  private def createDirectory(root: String): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, TEMP_DIR_PREFIX + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir
  }

  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  // Register the path to be deleted via shutdown hook
  private def registerShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

}
