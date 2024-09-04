package it.unibo.big.streamanalysis.input

/**
 * Definition of the data types
 */
object DataDefinition {

  sealed trait Data[T] {
    val value: T
  }

  def parse(x: String): Data[_] = {
    x match {
      case null | "null" | "" | "[]" | "{}" => NullData
      case v if util.Try(v.replace("\"", "").toDouble).toOption.nonEmpty => NumericData(v.replace("\"", "").toDouble)
      case v => StringData(v)
    }
  }

  /**
   * Numeric data
   * @param value the value
   */
  case class NumericData(value: Number) extends Data[Number] {
    /**
     * @return the value as double
     */
    def get: Double = value.doubleValue()
  }

  /**
   * String data
   * @param value the value
   */
  case class StringData(value: String) extends Data[String]

  /**
   * Null data
   */
  case object NullData extends Data[Null] {
    override val value: Null = null
  }
}
