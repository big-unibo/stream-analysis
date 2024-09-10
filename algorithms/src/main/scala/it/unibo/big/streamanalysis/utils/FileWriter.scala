package it.unibo.big.streamanalysis.utils

import java.io.File

/** Helper for write files */
object FileWriter {

  /**
   * File writer
   * @param filename name of the file
   * @param writeFun the function for write contents in the file
   * @param synchronized a boolean file that if true tells to apply a synchronized writing
   */
  private def writeFile(filename: String, writeFun : java.io.File => Unit, synchronized: Boolean = false): Unit = {
    val file = new java.io.File ( filename )
    if (!file.exists ()) {
      file.getParentFile.mkdirs
      file.createNewFile
    }
    if(synchronized) {
      file.getCanonicalPath.intern().synchronized{
        writeFun(file)
      }
    } else {
      writeFun(file)
    }
  }

  import com.github.tototoshi.csv.CSVWriter

  /**
   * Appends contents to the csv file
   * @param file the input file
   * @param writeFun the write function
   */
  private def appendCsv(file: java.io.File, writeFun : CSVWriter => Unit): Unit = writeCsv(file, writeFun, append = true)

  /**
   * A csv writer.
   *
   * @param file the input file
   * @param writeFun the write function
   * @param append a boolean parameter that if true tells to append the content
   */
  private def writeCsv(file: java.io.File, writeFun : CSVWriter => Unit, append: Boolean = false): Unit = {
    val writer = CSVWriter.open(file, append = append)
    writeFun(writer)
    writer.close()
  }

  /**
   * Method for write synchronously and more than once a csv with an initial header
   *
   * @param data the data to write
   * @param header the csv header
   * @param fileName the csv file name
   * @param synchronized true if want synchronized writing, default true
   * @param overwrite true if want to overwrite the file, default false
   */
  def writeFileWithHeader(data: Seq[Seq[Any]], header: Seq[Any], fileName: String, synchronized : Boolean = true, overwrite: Boolean = false): Unit = {
    val fileExists = new File(fileName).exists()
    writeFile(fileName, f => if (fileExists && !overwrite) {
      appendCsv(f, _.writeAll(data))
    } else {
      writeCsv(f, _.writeAll(Seq(header) ++ data))
    }, synchronized)
  }


}
