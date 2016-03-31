package it.eng.exabeat.test.commons

/**
  * @author Antonio Murgia 
  * @version 25/03/16
  */
trait ResourceAccessor {
  def getRelativeResourcePath(fileName: String = ""): String =
    getClass.getResource(s"${this.getClass.getSimpleName}Resources/${fileName}").getFile
  def getAbsoluteResourcePath(fileName: String): String =
    getClass.getClassLoader.getResource(fileName).getFile
}
