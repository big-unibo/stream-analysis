package it.unibo.big.query.generation.countdistinct

import it.unibo.big.input.DataDefinition.{Data, NullData}
import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.input.{AlgorithmConfiguration, ConfigurationUtils, StreamAnalysisConfiguration}
import it.unibo.big.query.generation.countdistinct.DimensionStatistics.{DimensionStatistic, DimensionVsDimensionStatistic, Statistic}
import org.slf4j.{Logger, LoggerFactory}

/**
 * CountDistinct is helper module for calculating the count distinct of dimensions
 */
object CountDistinct {

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)


  /**
   * Calculate the count distinct of dimensions
   * @param data the input data
   * @param configuration the configuration, if approximate is true the count distinct is approximated
   * @param calculateAlsoSupport if true the support is calculated
   * @param dimensions the dimensions to consider
   * @param groupingFunction the function to group the data
   * @param createStatistic the function to create the statistic
   * @return the statistics of the dimensions specified by the grouping function
   */
  private def calculateCardinality[K, V, T <: Statistic](data: Seq[Record], configuration: AlgorithmConfiguration, calculateAlsoSupport: Boolean, dimensions: Set[K], groupingFunction: (K, Record) => (V, Int), createStatistic: (Double, Long) => T): Map[K, T] = {
    // Group by key and consider null as a value
    val groupedData = data.flatMap(r => dimensions.map(d => d -> groupingFunction(d, r))).groupBy(_._1)

      groupedData.map { case (dim, seq) => dim -> createStatistic(
        if(calculateAlsoSupport) seq.map(_._2._2).sum.toDouble / data.size  else Double.NaN,
        seq.map(_._2._1).distinct.size.toLong)
      }
  }

  /**
   * Calculate the count distinct of dimensions
   * @param data the input data
   * @param dataDimensions the dimensions to consider
   * @param configuration the configuration, if approximate is true the count distinct is approximated
   * @param calculateAlsoSupport if true the support is calculated
   * @return the statistics of the dimensions
   */
  private def calculateDimensionCountDistinct(data: Seq[Record], dataDimensions: Set[String], configuration: AlgorithmConfiguration, calculateAlsoSupport: Boolean): Map[String, DimensionStatistic] = {
    calculateCardinality[String, Data[_], DimensionStatistic](data, configuration, calculateAlsoSupport, dataDimensions, (dim, d) => {
      val v = getDataAsNullValue(d, dim)
      (v, if (v != NullData) 1 else 0)
    }, (support, score) => DimensionStatistic(support, score, isElected = false))
  }

  /**
   * Calculate the count distinct of the pairs of dimensions
   * @param data the input data
   * @param configuration the configuration, if approximate is true the count distinct is approximated
   * @param pairsSet the pairs of dimensions to consider
   * @param dimensionCountDistinct the count distinct of the dimensions
   * @param calculateAlsoSupport if true the support is calculated
   * @return the statistics of the pairs of dimensions
   */
  private def getPairCountDistinct(data: Seq[Record], configuration: AlgorithmConfiguration, pairsSet: Set[(String, String)], dimensionCountDistinct: Map[String, DimensionStatistic], calculateAlsoSupport: Boolean): Map[(String, String), DimensionVsDimensionStatistic] = {
    val (simplePairs, complexPairs) = pairsSet.partition{
      case (d1, d2) if dimensionCountDistinct.contains(d1) && dimensionCountDistinct.contains(d2) =>
        dimensionCountDistinct(d1).countD <= 1 || dimensionCountDistinct(d2).countD <= 1
      case _ => true
    }

    val simplePairsResult = simplePairs.collect{
      case (d1, d2) if !dimensionCountDistinct.contains(d1) && !dimensionCountDistinct.contains(d2) =>
        (d1, d2) -> DimensionVsDimensionStatistic(Double.NaN, 1L)
      case (d1, d2) if dimensionCountDistinct.contains(d1) && dimensionCountDistinct.contains(d2) =>
        if(dimensionCountDistinct(d1).countD <= 1) {
          (d1, d2) -> DimensionVsDimensionStatistic(if (calculateAlsoSupport) dimensionCountDistinct(d2).support else Double.NaN, dimensionCountDistinct(d2).countD)
        } else { //} if (dimensionCountDistinct(d2).countD <= 1){
          require(dimensionCountDistinct(d2).countD <= 1, s"Dimension $d2 has count distinct ${dimensionCountDistinct(d2).countD} it must be <= 1")
          (d1, d2) -> DimensionVsDimensionStatistic(if (calculateAlsoSupport) dimensionCountDistinct(d1).support else Double.NaN, dimensionCountDistinct(d1).countD)
        }
      case (d1, d2) if dimensionCountDistinct.contains(d1) =>
        (d1, d2) -> DimensionVsDimensionStatistic(if (calculateAlsoSupport) dimensionCountDistinct(d1).support else Double.NaN, dimensionCountDistinct(d1).countD)
      case (d1, d2) if dimensionCountDistinct.contains(d2) =>
        (d1, d2) -> DimensionVsDimensionStatistic(if (calculateAlsoSupport) dimensionCountDistinct(d2).support else Double.NaN, dimensionCountDistinct(d2).countD)
    }.toMap

    val complexPairsResult = calculateCardinality[(String,String), (Data[_], Data[_]), DimensionVsDimensionStatistic](data, configuration, calculateAlsoSupport, complexPairs,
      (dims, d) => {
        val d1 = dims._1
        val d2 = dims._2
        val v1 = getDataAsNullValue(d, d1)
        val v2 = getDataAsNullValue(d, d2)
        ((v1, v2), if (v1 != NullData && v2 != NullData) 1 else 0)
      }, (support, score) => DimensionVsDimensionStatistic(support, score))

    simplePairsResult ++ complexPairsResult
  }

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
