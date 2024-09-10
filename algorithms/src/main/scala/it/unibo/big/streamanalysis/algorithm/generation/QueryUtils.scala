package it.unibo.big.streamanalysis.algorithm.generation

import it.unibo.big.streamanalysis.algorithm.generation.choosing.ScoreUtils.{AssignmentScore, DimensionScore, Score}
import it.unibo.big.streamanalysis.algorithm.generation.countdistinct.DimensionStatistics.{DimensionStatistic, DimensionVsDimensionStatistic}
import it.unibo.big.streamanalysis.algorithm.generation.countdistinct.ShlosserEstimator
import it.unibo.big.streamanalysis.algorithm.generation.functionaldependencies.HungarianMatcher
import it.unibo.big.streamanalysis.algorithm.state
import it.unibo.big.streamanalysis.algorithm.state.{QueryStatisticsInPane, State}
import it.unibo.big.streamanalysis.input.GPSJConcepts.Operators.MeasureOperator.aggregationFunctions
import it.unibo.big.streamanalysis.input.GPSJConcepts.{Aggregation, GPQuery, Projection}
import it.unibo.big.streamanalysis.input.RecordModeling.Record
import it.unibo.big.streamanalysis.input.{AlgorithmConfiguration, NaiveConfiguration, SimulationConfiguration}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random

/**
 * QueryUtils is a helper module for query related operations
 */
object QueryUtils {

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * Type for the queries with
   * - the score of the pane w.r.t. the previous query,
   * - the estimated number of estimatedRecords
   * - the total score w.r.t. the previous query
   */
  type QueriesWithStatistics = Map[GPQuery, QueryStatisticsInPane]

  /**
   * Get the number of estimatedRecords of a query, using the cardenas formula (if needed)
   * @param query                          the query
   * @param dimensionsStatistics           the dimensions statistics
   * @param data                           the data
   * @param dimensionVsDimensionStatistics the dimensions vs dimensions statistics
   * @param conf                           the configuration
   * @return the number of estimatedRecords estimated
   */
  private def getEstimatedNumberOfRecords(query: GPQuery, dimensionsStatistics: Map[String, DimensionStatistic], data: Seq[Record], dimensionVsDimensionStatistics: Map[(String, String), DimensionVsDimensionStatistic], conf: AlgorithmConfiguration): Long = {
    var dimensions = query.dimensions
    val dimensionsFd = dimensions.flatMap(d1 => dimensions.collect{
      case d2 if d1 < d2 && dimensionVsDimensionStatistics((d1, d2)).functionalDependencyScore.getOrElse(0D) >= 1.0 =>
        val cd1 = dimensionsStatistics(d1).countD
        val cd2 = dimensionsStatistics(d2).countD
        (d1, d2) -> (if(cd1 > cd2) d1 else d2)
    }).toMap
    var stop = false
    while (dimensions.nonEmpty && !stop) {
      val dimensionPair = dimensions.flatMap(d1 => dimensions.map(d2 => (d1, d2)))
      val dimensionsFdFiltered = dimensionsFd.filterKeys(dimensionPair.contains)
      if(dimensionsFdFiltered.nonEmpty) {
        dimensions = dimensions.filter(d => !dimensionsFdFiltered.exists(p => p._1._1 == d || p._1._2 == d)) ++ dimensionsFdFiltered.values
      } else {
        stop = true
      }
    }

    if(query.dimensions.size > dimensions.size) {
      LOGGER.debug(s"Starting dimensions = ${query.dimensions.mkString(",")} removed dimensions = ${query.dimensions.diff(dimensions).mkString(",")}")
    }

    dimensions.size match {
      //if the dimensions are 1 or 2 can use previous calculations
      case 1 => dimensionsStatistics(dimensions.head).countD
      case 2 =>
        val sortedDimensions = dimensions.toSeq.sorted
        util.Try(dimensionVsDimensionStatistics((sortedDimensions.head, sortedDimensions.last)).countD).getOrElse(dimensionsStatistics(dimensions.head).countD * dimensionsStatistics(dimensions.last).countD)
      case _ =>
        val gp = dimensions.toSeq.map(d => dimensionsStatistics(d).countD).product.toDouble
        val ep = data.size.toDouble
        Math.ceil(gp * (1 - math.pow(1 - 1 / gp, ep))).toLong
    }
  }

  /**
   * Calculate the support of a query
   *
   * @param query the query
   * @param dimensionsStatistics the dimensions statistics
   * @param dimensionsVsdimensionsStatistics the dimensions vs dimensions statistics
   * @return the support of the query as the ratio of the number of records that have all the dimensions of the query
   */
  private def calculateQuerySupport(query: GPQuery, dimensionsStatistics: Map[String, DimensionStatistic], dimensionsVsdimensionsStatistics: Map[(String, String), DimensionVsDimensionStatistic]): Double = {
    val querySupport = query.dimensions.size match {
      case 1 if dimensionsStatistics.contains(query.dimensions.head) => dimensionsStatistics(query.dimensions.head).support
      case 2 if dimensionsVsdimensionsStatistics.get((query.dimensions.head, query.dimensions.last)).exists(x => !x.support.isNaN) => dimensionsVsdimensionsStatistics((query.dimensions.head, query.dimensions.last)).support
      case _ => query.dimensions.toSeq.map(dim => util.Try(dimensionsStatistics(dim).support).getOrElse(0D)).product
    } //val querySupport = data.map(d => query.dimensions.forall(dim => d.data.getOrElse(dim, NullData) != NullData)).count(_ == true).toDouble / data.size
    require(querySupport >= 0 && querySupport <= 1, s"Query support must be in [0,1] but is $querySupport")
    querySupport
  }

  /**
   * @param dimension1 the first dimension with its count distinct
   * @param dimension2 the second dimension with its count distinct
   * @param countDistinctDim1Dim2 the count distinct of the dimensions
   * @return the FD score between the two dimensions
   */
  def calculateFDsScore(dimension1: (String, Long), dimension2: (String, Long), countDistinctDim1Dim2: Long): Double = {
    var scoreStrength = List(dimension1._2.toDouble / countDistinctDim1Dim2, dimension2._2.toDouble / countDistinctDim1Dim2)
      .map(x => if(x.isNaN || x.isInfinity) 0D else x).max
    if(scoreStrength > 1) {
      LOGGER.warn(s"Score is higher than 1 for ${dimension1._1} and ${dimension2._1} with countd d1 = ${dimension1._2} d2 = ${dimension2._2} pair = $countDistinctDim1Dim2, setting to 1")
      scoreStrength = 1
    }
    require(scoreStrength >= 0 && scoreStrength <= 1, s"Score must be in [0,1] but is $scoreStrength")
    scoreStrength
  }

  /**
   * @param data the data
   * @param algorithmState the algorithm state
   * @param inputQuery the input query, if present
   * @param paneTime the time of the pane
   * @param dimensions the dimensions of the data
   * @param measures the measures of the data
   * @param simulationConfiguration the simulation configuration
   * @return the the map of queries with score
   */
  def updateQueriesStatistics(data: Seq[Record], algorithmState: State, inputQuery: Option[GPQuery], paneTime: Long, dimensions: Set[String], measures: Set[String], simulationConfiguration: SimulationConfiguration): Unit = {
    val startTime = System.currentTimeMillis()
    val configuration = algorithmState.configuration
    val calculateStatisticsJustForInputQuery = configuration.isInstanceOf[NaiveConfiguration] && configuration.maximumQueryCardinalityPercentage == 1D
    var time = System.currentTimeMillis()
    val (dimensionVsDimensionStatistics, allDimensionsStatistics, functionalDependenciesForTheInputQuery) = ShlosserEstimator.calculateFunctionalDependencies(data, configuration, inputQuery, dimensions, calculateOnlyForInputQuery = calculateStatisticsJustForInputQuery)
    val dimensionsStatistics = if(calculateStatisticsJustForInputQuery) allDimensionsStatistics else allDimensionsStatistics.filter(_._2.isElected)
    LOGGER.info(s"Time to calculate functional dependencies: ${System.currentTimeMillis() - time}")
    // filter the dimensionsCountD considering the dimensions that are not filtered out from the functional dependencies
    time = System.currentTimeMillis()
    //not consider measures that are dimensions for some data, consider the sample for define measures
    val queryAggregation = measures.flatMap(a => aggregationFunctions.map(f => Aggregation(a, f)))
    LOGGER.debug(s"Query aggregation: $queryAggregation - elapsed time: ${System.currentTimeMillis() - time}")

    time = System.currentTimeMillis()
    //generate the combinations of dimensions under k attributes in the group by set
    val combinations = dimensionsStatistics.keys.toSeq.combinations(configuration.k).toSeq
      //if the pair is in the query and not in the dependencies I need to filter the query out
      .filter(dimensions => if(calculateStatisticsJustForInputQuery) true else dimensions.forall(d1 => dimensions.filter(_ > d1).forall(d2 => dimensionVsDimensionStatistics.contains((d1, d2)))))
      //filter based on exclude/include dimensions
      .map(c => GPQuery(c.map(d => Projection(d)).toSet, queryAggregation))
    LOGGER.debug(s"Time to generate combinations: ${System.currentTimeMillis() - time}")

    //add number of estimatedRecords
    time = System.currentTimeMillis()
    val queriesWithEstimatedNumberOfRecords: Map[GPQuery, Long] = if(calculateStatisticsJustForInputQuery) combinations.map(q => q -> 0L).toMap else getFilteredQueriesWithEstimatedNumberOfRecords(data, configuration, dimensionsStatistics, combinations, dimensionVsDimensionStatistics)
    LOGGER.debug(s"Time to calculate number of estimatedRecords: ${System.currentTimeMillis() - time}")

    //for each query calculate the score
    time = System.currentTimeMillis()
    var timeForCalculateSupport = 0L
    var timeForCalculateFdScore = 0L
    val fdLastPane: Map[(String, String), Double] = functionalDependenciesForTheInputQuery ++ dimensionVsDimensionStatistics.mapValues(_.functionalDependencyScore).filter(_._2.nonEmpty).mapValues(_.get)

    val queryCardinalities: Map[Long, Map[GPQuery, Long]] = algorithmState.getQueryCardinalities + (paneTime -> queriesWithEstimatedNumberOfRecords)

    var queriesWithStatistics: QueriesWithStatistics = queriesWithEstimatedNumberOfRecords.map {
      case (q, estimatedRecords) =>
        var t = System.currentTimeMillis()
        val support = calculateQuerySupport(q, dimensionsStatistics, dimensionVsDimensionStatistics)
        timeForCalculateSupport += System.currentTimeMillis() - t
        //calculate the fd scores for each combination of dimensions
        t = System.currentTimeMillis()
        val score = Score(algorithmState, inputQuery, paneTime, q, Some(support), queryCardinalities, fdLastPane)
        timeForCalculateFdScore += System.currentTimeMillis() - t
        q -> QueryStatisticsInPane(q, estimatedRecords, score, configuration.timeForQueryComputation(simulationConfiguration, configuration.k, simulationConfiguration.availableTime))
    }
    if(inputQuery.isDefined) {
      if(!queriesWithStatistics.contains(inputQuery.get)) {
        val totalInputQueryScore = Score(algorithmState, inputQuery, paneTime, inputQuery.get, Some(0D), queryCardinalities, fdLastPane)
        queriesWithStatistics += inputQuery.get -> state.QueryStatisticsInPane(inputQuery.get, estimatedNumberOfRecords = 0, scoreValue = totalInputQueryScore, 0L, exists = false)
      }
    }
    LOGGER.debug(s"Time to calculate support for all queries: $timeForCalculateSupport")
    LOGGER.debug(s"Time to calculate score functional dependency for all queries: $timeForCalculateFdScore")

    algorithmState.getTimeStatistics.addTimeForScoreComputation(System.currentTimeMillis() - startTime)
    //add this results in the state
    algorithmState.addQueriesThatCanBeExecuted(queriesWithStatistics, fdLastPane, paneTime)
  }

  /**
   *
   * @param data the data
   * @param configuration the configuration
   * @param dimensionsStatistics the dimensions statistics
   * @param combinations the combinations of the queries that can be executed
   * @param dimensionVsDimensionStatistics the dimensions vs dimensions statistics
   * @return the queries with the estimated number of records, filtered by the maximum number of estimatedRecords (if present)
   */
  private def getFilteredQueriesWithEstimatedNumberOfRecords(data: Seq[Record], configuration: AlgorithmConfiguration,
                                                             dimensionsStatistics: Map[String, DimensionStatistic], combinations: Seq[GPQuery],
                                                             dimensionVsDimensionStatistics : Map[(String, String), DimensionVsDimensionStatistic]): Map[GPQuery, Long] = {
    combinations.map(q => q -> getEstimatedNumberOfRecords(q, dimensionsStatistics, data, dimensionVsDimensionStatistics, configuration)).toMap
  }

  /**
   * Get a sample of the data
   * @param data the data
   * @param percentage the percentage of the sample (default 0.1)
   * @return the sample of the data
   */
  def getSample(data: Seq[Record], percentage : Double = 0.1): Seq[Record] = {
    require(percentage > 0 && percentage <= 1, s"Percentage must be in ]0,1] but is $percentage")
    // Calculate the number of elements to sample (percentage of the original size)
    val sampleSize: Int = (data.size * percentage).toInt
    Random.shuffle(data).take(sampleSize)
  }

  /**
   * Get the FD score for the input query and the query
   *
   * @param inputQuery the input query
   * @param q the query
   * @param functionalDependenciesScore the score of functional dependencies for dimensions vs dimensions
   * @return the FD score for the input query and the query
   */
  def getFdScore(inputQuery: Option[GPQuery], q: GPQuery, functionalDependenciesScore: Map[(String, String), Double]): Set[DimensionScore] = {
    if (inputQuery.isDefined) {
      //for each combinations of dimension of input query and other query calculate the assignment score
      val assignmentScores = inputQuery.get.dimensions.flatMap(x => q.dimensions.map(y => (x, y))).collect {
        case (q_input, q_dim) if q_input == q_dim => (q_dim, q_input) -> AssignmentScore(q_dim, q_input, 1D)
        case (q_input, q_dim) if q_input < q_dim && functionalDependenciesScore.contains((q_input, q_dim))  =>
          (q_input, q_dim) -> AssignmentScore(q_input, q_dim, functionalDependenciesScore((q_input, q_dim)))
        case (q_input, q_dim) if q_dim < q_input && functionalDependenciesScore.contains((q_dim, q_input)) =>
          (q_input, q_dim) -> AssignmentScore(q_input, q_dim, functionalDependenciesScore((q_dim, q_input)))
      }.toMap
      // Use HungarianMatcher to find the best assignment
      val hungarianMatcherResult = util.Try(HungarianMatcher.solve(assignmentScores).toSet[DimensionScore]).toOption.getOrElse(Set())
      if(hungarianMatcherResult.size == q.dimensions.size) hungarianMatcherResult else Set()
    } else Set()
  }
}
