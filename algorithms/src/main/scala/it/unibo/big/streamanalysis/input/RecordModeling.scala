package it.unibo.big.streamanalysis.input

object RecordModeling {
  import java.sql.Timestamp

  /**
   * Window definition
   *
   * @param start time
   * @param end   time
   * @param period the period of the window, to slide
   * @param isFirstCompleteWindow true if the window is the first complete window, false otherwise (default is false)
   */
  case class Window(start: Timestamp, end: Timestamp, period: Long, isFirstCompleteWindow: Boolean = false) {

    /**
     *
     * @param schema a timestamped schema
     * @return an option that tells the pane start of that schema, if the schema is in that window
     */
    def paneStart(schema: Record): Option[Long] = {
      val t = schema.timestamp.getTime
      if (!contains(t)) {
        None
      } else {
        var paneStart = start.getTime
        var paneEnd = paneStart + period
        var find = false
        while (paneEnd <= end.getTime && !find) {
          if (t >= paneStart && t < paneEnd) {
            find = true
          }
          if (!find) {
            paneStart = paneEnd
            paneEnd += period
          }
        }
        if (find) Some(paneStart) else None
      }
    }

    /**
     *
     * @return a slide window: add period duration to both start and end time
     */
    def slide: Window = Window(new Timestamp(start.getTime + period), new Timestamp(end.getTime + period), period)

    /**
     *
     * @param t a timestamp
     * @return true if the timestamp is in window interval
     */
    def contains(t: Timestamp): Boolean = contains(t.getTime)

    /**
     *
     * @param t a timestamp
     * @return true if the timestamp is in window interval
     */
    def contains(t: Long): Boolean = t >= start.getTime && t < end.getTime

    /**
     *
     * @return the time of the pane
     */
    def paneTime: Long = end.getTime - period

    /**
     *
     * @return the window length
     */
    def length: Long = end.getTime - start.getTime
  }

  import DataDefinition.{Data, NumericData, StringData}

  case class Record(data: Map[String, Data[_]], timestamp: Timestamp) {

    /**
     * The schema of the record
     */
    val schema: String = data.keys.toList.sorted.mkString(",")
    /**
     *
     * @return the data of the schema that is are string
     */
    def dimensions: Set[String] = data.collect{
      case (k, StringData(value)) if value != null => k
    }.toSet

    /**
     *
     * @return the data of the schema that is are not string
     */
    def measures: Set[String] = data.collect{
      case (k, NumericData(value)) if value != null => k
    }.toSet

    require(dimensions.intersect(measures).isEmpty, "The dimensions and measures must be disjoint")
  }
}
