package it.unibo.big.input

import it.unibo.big.input
import it.unibo.big.input.GPSJConcepts.QueryPattern
import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.query.app.DatasetsUtils.Dataset
import it.unibo.big.query.app.ExecuteTests
import it.unibo.big.query.app.ExecuteTests.OurExecutionAlgorithm
import it.unibo.big.query.execution.QueryExecutionTimeUtils

object ConfigurationUtils {

  /**
   *
   * @param data the number of values
   * @param logFactor the log factor
   * @return the logarithmically number of values w.r.t. the inputs
   */
  def getMaximumNumberOfRecordsToStore(data: Int, logFactor: Double): Int = {
    val maxNumberOfRecords = math.pow(math.log(data), logFactor)
    if(maxNumberOfRecords.isInfinity || maxNumberOfRecords.isNaN) 0 else math.max(math.ceil(maxNumberOfRecords).toInt, 0)
  }

  /**
   * Get the log factor for the maximum number of records
   * @param maximumNumberOfRecordsPercentage the percentage maximum number of records
   * @param data the data
   * @return the log factor
   */
  def getLogFactor(maximumNumberOfRecordsPercentage: Double, data: Int): Double = {
    require(maximumNumberOfRecordsPercentage <= 1 && maximumNumberOfRecordsPercentage > 0, "The percentage must be in ]0,1]")
    val maximumNumberOfRecords = math.ceil(data.toDouble * maximumNumberOfRecordsPercentage)
    val logData = math.log(data)
    val logFactor = math.log(maximumNumberOfRecords) / math.log(logData)
    logFactor
  }
}

/**
 * @param dataset the dataset
 * @param windowDuration the window duration
 * @param slideDuration the slide duration
 * @param numberOfWindowsToConsider the number of windows to consider in the simulation, optional
 * @param statisticsFile the statistics file where write the statistics about all the queries in the simulation
 * @param chosenQueryStatisticsFile the statistics file where write the statistics about the chosen query
 * @param datasetStatisticsFile the statistics file where write the statistics about the dataset
 * @param userPreferencesFile the user preferences file
 * @param availableTime the available time for the query execution
 */
case class SimulationConfiguration(dataset: Dataset, windowDuration: Long, slideDuration: Long, numberOfWindowsToConsider: Option[Int],
                                   statisticsFile: String,
                                   chosenQueryStatisticsFile: String,
                                   datasetStatisticsFile: String,
                                   userPreferencesFile: String, availableTime: Long) {
  require(windowDuration > 0 && windowDuration % slideDuration == 0, "window duration must be greater than 0 and multiple of slide duration")
  val numberOfPanes : Int = (windowDuration / slideDuration).toInt
  val frequency: Double = math.floor(slideDuration.toDouble / availableTime)
}

/**
 * Configuration trait for the algorithm, have two specializations the naive one and the algorithm one
 */
sealed trait AlgorithmConfiguration {
  val pattern: QueryPattern
  val alpha: Double
  val beta: Double = 1 - alpha
  val percentageOfRecordsInQueryResults: Double
  require(alpha + beta == 1, "alpha + beta must be equal to 1")

  if(pattern.maxRecords.isDefined) {
    require(pattern.maxRecords.get > 0, "max records must be greater than 0")
  }

  val timeForQueryComputation: (SimulationConfiguration, Int, Long) => Long
  require(percentageOfRecordsInQueryResults > 0 && percentageOfRecordsInQueryResults <= 1, "percentage of records in query result must be in ]0,1]")

  val name: String = this match {
    case _: NaiveConfiguration => "NAIVE"
    case x: StreamAnalysisConfiguration if x.singleQuery && x.knapsack.isEmpty => "A-S1"
    case x: StreamAnalysisConfiguration if x.knapsack.isEmpty => "A-SE"
    case x: StreamAnalysisConfiguration if  x.knapsack.nonEmpty => "A-SKE"
  }
}

/**
 *
 * @param alpha the alpha parameter, for weighting the support in the score
 * @param queryPattern the query pattern
 * @param percentageOfRecordsInQueryResults the percentage of records to use as threshold for the count distinct of dimensions
 */
case class ConfigurationSetting(alpha: Double, queryPattern: QueryPattern, percentageOfRecordsInQueryResults: Double) {

  /**
   * Check if the configuration is compliant with the given algorithm configuration
   * @param algorithmConfiguration the algorithm configuration
   */
  def isCompliantConfiguration(algorithmConfiguration: AlgorithmConfiguration): Unit = {
    require(alpha == algorithmConfiguration.alpha, "alpha must be the same")
    require(queryPattern == algorithmConfiguration.pattern, "query pattern must be the same")
    require(percentageOfRecordsInQueryResults == algorithmConfiguration.percentageOfRecordsInQueryResults, "percentage of records in query result must be the same")
  }
}
/**
 * Configuration class for naive execution
 * @param pattern the query pattern
 * @param alpha the alpha parameter, for weighting the support in the score
 * @param percentageOfRecordsInQueryResults the percentage of records to use as threshold for the count distinct of dimensions
 * @param timeForQueryComputation the time for compute a query given a simulation configuration and the remaining time
 */
case class NaiveConfiguration(pattern: QueryPattern,
                              alpha: Double,
                              percentageOfRecordsInQueryResults: Double,
                              timeForQueryComputation: (SimulationConfiguration, Int, Long) => Long) extends AlgorithmConfiguration

object NaiveConfiguration {
  def apply(pattern: QueryPattern, alpha: Double, percentageOfRecordsInQueryResults: Double = 0.05): NaiveConfiguration = input.NaiveConfiguration(
    pattern = pattern,
    alpha = alpha,
    percentageOfRecordsInQueryResults = percentageOfRecordsInQueryResults,
    timeForQueryComputation = QueryExecutionTimeUtils.getExecutionTime
  )
}

/**
 * Configuration class
 *
 * @param pattern the query pattern
 * @param alpha the alpha parameter, for weighting the support in the score
 * @param timeForQueryComputation the time for compute a query given a simulation configuration and the remaining time
 * @param logFactor the log factor for the maximum number of records to store
 * @param knapsack if the knapsack algorithm should be used it is not empty, the double is the percentage of maximum records to add to the knapsack (e.g., 1.5)
 * @param percentageOfRecordsInQueryResults the percentage of records to use as threshold for the count distinct of dimensions
 * @param singleQuery the algorithm can execute just a query
 */
case class StreamAnalysisConfiguration(pattern: QueryPattern,
                                       alpha: Double,
                                       timeForQueryComputation: (SimulationConfiguration, Int, Long) => Long, logFactor: Double, knapsack: Option[Double], percentageOfRecordsInQueryResults: Double = 0.05,
                                       singleQuery: Boolean) extends AlgorithmConfiguration {

  def getMaximumOfRecordsToStore(data: Seq[Record]): Int = {
    ConfigurationUtils.getMaximumNumberOfRecordsToStore(data.size, logFactor)
  }
}

object StreamAnalysisConfiguration {
  /**
   * Get the configurations for the given parameters
   * @param queryExecutionTime the query execution time
   * @param queryPattern the query pattern
   * @param alphas the alphas values
   * @param logFactors the log factors to consider
   * @param algorithms the algorithms to consider
   * @param getPercentage function for the percentage of records in the query results from logFactor, default is 0.05
   * @return the configurations
   */
  def getConfigurations(queryExecutionTime: (SimulationConfiguration, Int, Long) => Long, queryPattern: QueryPattern, alphas: Set[Double], logFactors: Set[Double],
                        algorithms: Set[OurExecutionAlgorithm], getPercentage: Double => Double = _ => 0.05): Seq[StreamAnalysisConfiguration] = {
    alphas.toSeq.flatMap(alpha => {
      logFactors.toSeq.flatMap(logFactor => algorithms.map(a => {
        val (knapsack, singleQuery) = a match {
          case ExecuteTests.ASKE => (Some(1D), false)
          case ExecuteTests.ASE => (None, false)
          case ExecuteTests.AS1 => (None, true)
        }
        StreamAnalysisConfiguration(
          pattern = queryPattern,
          alpha = alpha,
          timeForQueryComputation = queryExecutionTime,
          logFactor = logFactor,
          knapsack = knapsack,
          percentageOfRecordsInQueryResults = getPercentage(logFactor),
          singleQuery = singleQuery
        )
      }))
    })
  }
}