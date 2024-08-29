package it.unibo.big.input

import java.io.File

object UserPreferences {

  /**
   * User preferences
   * @param includeDimensions the dimensions to include
   * @param excludeDimensions the dimensions to exclude
   */
  case class UserPreferences(includeDimensions: Set[String] = Set(), excludeDimensions: Set[String] = Set())

  import scala.io.Source
  // Function to parse user-defined dimension file
  def parseUserPreferences(filePath: String): UserPreferences = {
    var includeDimensions = Set[String]()
    var excludeDimensions = Set[String]()

    val file = new File(filePath)
    if (!file.exists()) {
      UserPreferences()
    } else {
      val fileSource = Source.fromFile(file)
      for (line <- fileSource.getLines()) {
        val trimmedLine = line.trim()
        if (trimmedLine.nonEmpty && !trimmedLine.startsWith("#")) {
          if (trimmedLine.startsWith("+")) {
            includeDimensions += trimmedLine.substring(1)
          } else if (trimmedLine.startsWith("-")) {
            excludeDimensions += trimmedLine.substring(1)
          }
        }
      }
      fileSource.close()
      UserPreferences(includeDimensions, excludeDimensions)
    }
  }
}
