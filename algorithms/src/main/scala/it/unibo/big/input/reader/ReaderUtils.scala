package it.unibo.big.input.reader

import it.unibo.big.input.DataDefinition
import it.unibo.big.input.DataDefinition._
import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.utils.FileReader
import it.unibo.big.utils.FileReader.MyConverter

import java.sql.Timestamp

object ReaderUtils {
  /**
   *
   * @param fileName the file name
   * @param readAsAStream if true lazy reading of the file considering it already sorted by time and value
   * @return the iterator of the file with data
   */
  def readRecords(fileName: String, readAsAStream: Boolean): Iterator[Record] = {
    if(readAsAStream) FileReader.readLazyFile(fileName, isResource = false)(recordWithoutSeedConverter)
    else FileReader.readFile(fileName, isResource = false)(recordWithoutSeedConverter).sortBy(x => (x.timestamp.getTime, x.schema)).toIterator
  }

  private val splitRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

  /**
   * Converter for the simulation with data input with Record
   */
  private def recordWithoutSeedConverter: MyConverter[Record] = new MyConverter[Record] {
    override def parse(line: String): Record = {
      val Array(timestamp, data) = line.split(splitRegex).map(_.trim)
      //want to split a map of values "HL=58, IL=73, JL=37, KL=11, LL=42, AM=51, CM=47, DM=81" into a map string, any
      val dataValues : Map[String, Data[_]] = {
        data.split(",").map(_.replaceAll("\"", "").trim().split("="))
          .map(x => {
            try {
              x(0) -> DataDefinition.parse(x(1))
            } catch {
              case e: Exception =>
                e.printStackTrace()
                x(0) -> NullData
            }
          }).toMap
      }
      Record(dataValues, new Timestamp(timestamp.toLong))
    }
  }
}
