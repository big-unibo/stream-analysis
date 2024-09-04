package it.unibo.big.query.generation.countdistinct

import it.unibo.big.input.AlgorithmConfiguration
import it.unibo.big.input.DataDefinition.{Data, NullData}
import it.unibo.big.input.GPSJConcepts.GPQuery
import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.query.generation.QueryUtils.{calculateFDsScore, getSample}
import it.unibo.big.query.generation.countdistinct.CountDistinct.{filterMapWithCountDistinct, getDataAsNullValue}
import it.unibo.big.query.generation.countdistinct.DimensionStatistics.{DimensionStatistic, DimensionVsDimensionStatistic, Statistic}
import org.slf4j.{Logger, LoggerFactory}

object ShlosserEstimator {

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * Estimate the count of distinct values for each dimension
   * @param records the records
   * @param dimensions the dimensions
   * @param calculateAlsoSupport if true also calculate the support
   * @param samplePercentage the sample percentage
   * @param groupingFunction the grouping function
   * @param createStatistic the statistic creation function
   * @tparam K the key type of the dimension
   * @tparam V the value type of the dimension
   * @tparam T the statistic type
   * @return the results in the sample and the estimated count of distinct values for each dimension in the dataset
   */
  private def estimateCount[K, V, T <: Statistic](records: Seq[Record], dimensions: Set[K],  calculateAlsoSupport: Boolean, groupingFunction: (K, Record) => (V, Int), createStatistic: (Double, Long) => T, samplePercentage : Double = 0.1): (Map[K, (Double, Map[Int, Int], Int)], Map[K, T]) = {
    val sample = getSample(records, samplePercentage)
    val sampleResult = sample.flatMap(r => dimensions.map(d => d -> groupingFunction(d, r))).groupBy(_._1)
      .map{
        case (d, values) =>
          val support = if(calculateAlsoSupport) values.map(_._2._2).sum.toDouble / sample.size  else Double.NaN
          val distinctValues = values.map(_._2._1).toSet.size
          val fs: Map[Int, Int] = values.groupBy(_._2._1).toSeq.map{
            case (v, vs) => v -> vs.size
          }.groupBy(_._2).map{
            case (i, vx) => i -> vx.size
          }
          require(fs.values.sum == distinctValues, s"fs values sum ${fs.values.sum} is different from distinct values $distinctValues")
          require(fs.toSeq.map{
            case (i, f) => i*f
          }.sum == sample.size, s"fs values sum ${fs.toSeq.map{
            case (i, f) => i*f
          }.sum} is different from sample size ${sample.size}")
          d -> (support, fs, distinctValues)
      }
    (sampleResult, sampleResult.map{
        case (d, (support, fs, distinctValues)) =>
          val num = fs.getOrElse(1, 0) + fs.map{
            case (i, v) => math.pow(1 - samplePercentage, i) * v
          }.sum
          val den = fs.map{
            case (i, v) => i * samplePercentage * math.pow(1 - samplePercentage, i - 1) * v
          }.sum
          val estimatedCardinality = math.ceil(distinctValues + (num / den)).toLong
          d -> createStatistic(support, estimatedCardinality)
      })
  }

  /**
   * Calculate the count distinct of dimensions
   *
   * @param data the input data
   * @param configuration the configuration, if approximate is true the count distinct is approximated
   * @param inputQuery the input query
   * @param dataDimensions the dimensions in the data
   * @param calculateOnlyForInputQuery if false the calculation is made just for the input query (default false)
   * @return - a map for each pair of dimensions that are valid with the functional dependencies scores and count distinct score,
   *         - a map where for each dimension there is the statistic of the dimension
   *         - the functional dependencies scores for each pair of dimensions considering the input query,
   */
  def calculateFunctionalDependencies(data: Seq[Record], configuration: AlgorithmConfiguration,
                                      inputQuery: Option[GPQuery], dataDimensions: Set[String], calculateOnlyForInputQuery: Boolean = false): (Map[(String, String), DimensionVsDimensionStatistic],
                                                                                                                                              Map[String, DimensionStatistic],
                                                                                                                                              Map[(String, String), Double]) = {
    if (calculateOnlyForInputQuery) {
      if (inputQuery.isEmpty) Map() -> Map
    }
    var time = System.currentTimeMillis()
    val (statsSampleDim: Map[String, (Double, Map[Int, Int], Int)], stringDataStatistics: Map[String, DimensionStatistic]) = estimateCount[String, Data[_], DimensionStatistic](records = data,
      dimensions = dataDimensions,
      calculateAlsoSupport = true,
      groupingFunction = (dim: String, d: Record) => {
        val v : Data[_] = getDataAsNullValue(d, dim)
        (v, if (v != NullData) 1 else 0)
      },
      createStatistic = (s: Double, c: Long) => DimensionStatistic(s, c, isElected = false))
    LOGGER.info(s"Time to calculate the count distinct of the dimensions ${System.currentTimeMillis() - time} ms - number of strings = ${stringDataStatistics.size}")


    val inputQueryDimensions = inputQuery.map(_.dimensions.filter(stringDataStatistics.contains)).getOrElse(Set())
    val dimensionsCountD: Map[String, DimensionStatistic] = filterMapWithCountDistinct[String, DimensionStatistic](stringDataStatistics, _.countD, (x, _) => x.support >= 0D, configuration, data.size, keepInputElement = d => inputQueryDimensions.contains(d))

    //define the pairs of dimensions to consider
    val pairsSet = if (calculateOnlyForInputQuery) {
      inputQueryDimensions //select only the dimensions that are in the current data
        .flatMap(d1 => dimensionsCountD.keys.collect {
          case d2 if d1 < d2 => (d1, d2)
          case d2 if d2 < d1 => (d2, d1)
        })
    } else {
      dimensionsCountD.keys.toSeq
        .flatMap(d1 => dimensionsCountD.keys.collect {
          case d2 if d1 < d2 => (d1, d2)
        }).toSet
    }

    time = System.currentTimeMillis()
    val (statsSampleDims, pairsDataStatistics) = estimateCount[(String, String), (Data[_], Data[_]), DimensionVsDimensionStatistic](records = data,
      dimensions = pairsSet,
      calculateAlsoSupport = false,
      (dims: (String, String), d: Record) => {
        val d1 = dims._1
        val d2 = dims._2
        val v1 = getDataAsNullValue(d, d1)
        val v2 = getDataAsNullValue(d, d2)
        ((v1, v2), if (v1 != NullData && v2 != NullData) 1 else 0)
      }, (s: Double, c: Long) => DimensionVsDimensionStatistic(s, c))

    LOGGER.info(s"Time to calculate the count distinct of the sample pairs ${System.currentTimeMillis() - time} ms")
    val filteredPairsCountD = filterMapWithCountDistinct[(String, String), DimensionVsDimensionStatistic](pairsDataStatistics,
      _.countD,
      keepElementConfiguration = (_, _) => true, //there is no support to check for pairs
      configuration = configuration,
      dataSize = data.size,
      keepInputElement = pair => inputQueryDimensions.contains(pair._1) || inputQueryDimensions.contains(pair._2))

    val dimensionsVsDimensionsStatistics = filteredPairsCountD.map {
      case ((d1, d2), ds) =>
        //use sample stats for compute the functional dependencies
        val fds = calculateFDsScore((d1, statsSampleDim(d1)._3), (d2,  statsSampleDim(d2)._3), statsSampleDims((d1, d2))._3)
        ((d1, d2), ds.setFunctionalDependencyScore(fds))
    }

    val dimensions = filteredPairsCountD.keys.flatMap(p => Seq(p._1, p._2)).toSet

    val functionalDependenciesScoresForInputQuery = pairsDataStatistics.filterKeys(pair =>
      (inputQueryDimensions.contains(pair._1) && dimensions.contains(pair._2)) ||
        (inputQueryDimensions.contains(pair._2) && dimensions.contains(pair._1))).map{
      case ((d1, d2), ds) => ((d1, d2), util.Try(dimensionsVsDimensionsStatistics((d1, d2)).functionalDependencyScore.get).getOrElse(calculateFDsScore((d1, dimensionsCountD(d1).countD), (d2, dimensionsCountD(d2).countD), ds.countD)))
    }
    val dimensionsStatistics = stringDataStatistics.map{
      case (d, DimensionStatistic(support, countD, _)) => d -> DimensionStatistic(support, countD, dimensions.contains(d))
    }

    (dimensionsVsDimensionsStatistics, dimensionsStatistics, functionalDependenciesScoresForInputQuery)
  }
}
