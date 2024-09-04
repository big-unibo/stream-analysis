package it.unibo.big.query.generation.countdistinct

import it.unibo.big.input.DataDefinition.{Data, NullData}
import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.input.{AlgorithmConfiguration, ConfigurationUtils, StreamAnalysisConfiguration}

/**
 * CountDistinct is helper module for calculating the count distinct of dimensions
 */
object CountDistinct {

  /**
   *
   * @param inputMap a map with some object
   * @param getValue a function that gives the value of the object
   * @param keepElementConfiguration a function that gives the support of the object
   * @param configuration the configuration
   * @param dataSize the size of the data
   * @param keepInputElement a function that gives a boolean I still to keep the element
   * @return the filtered map considering the configuration
   */
  def filterMapWithCountDistinct[T, V](inputMap : Map[T, V], getValue: V => Long, keepElementConfiguration: (V, AlgorithmConfiguration) => Boolean, configuration: AlgorithmConfiguration, dataSize: Int, keepInputElement: T => Boolean = (_: T) => false): Map[T, V] = {
    var filteredMap = inputMap.filter(x =>
      (getValue(x._2) <= dataSize * configuration.percentageOfRecordsInQueryResults && keepElementConfiguration(x._2, configuration))
        || keepInputElement(x._1))

    //filter out queries with more estimated records than the maximum allowed
    filteredMap = if (configuration.pattern.maxRecords.isDefined) filteredMap.filter(x => getValue(x._2) <= configuration.pattern.maxRecords.get || keepInputElement(x._1)) else filteredMap

    configuration match {
      case c: StreamAnalysisConfiguration =>
        //filter out queries with more estimated records than the maximum allowed
        val maximumNumberOfRecords = ConfigurationUtils.getMaximumNumberOfRecordsToStore(dataSize, c.logFactor)
        filteredMap.filter(x => getValue(x._2) <= maximumNumberOfRecords || keepInputElement(x._1))
      case _ => filteredMap
    }
  }

  def getDataAsNullValue(d: Record, key: String): Data[_] = d.data.getOrElse(key, NullData)
}
