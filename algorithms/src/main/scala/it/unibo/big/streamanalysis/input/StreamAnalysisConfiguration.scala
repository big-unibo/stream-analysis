package it.unibo.big.streamanalysis.input

import it.unibo.big.streamanalysis
import it.unibo.big.streamanalysis.algorithm.app.ExecutionUtils.{AS1, ASE, ASKE, OurExecutionAlgorithm}
import it.unibo.big.streamanalysis.algorithm.execution.QueryExecutionTimeUtils
import it.unibo.big.streamanalysis.input.RecordModeling.Record
import it.unibo.big.streamanalysis.utils.DatasetsUtils.Dataset

/**
 * @param dataset the dataset
 * @param windowDuration the window duration
 * @param slideDuration the slide duration
 * @param numberOfWindowsToConsider the number of windows to consider in the simulation, optional
 * @param statisticsFile the statistics file where write the statistics about all the queries in the simulation
 * @param datasetStatisticsFile the statistics file where write the statistics about the dataset
 * @param availableTime the available time for the query execution
 */
case class SimulationConfiguration(dataset: Dataset, windowDuration: Long, slideDuration: Long, numberOfWindowsToConsider: Option[Int],
                                   statisticsFile: String,
                                   datasetStatisticsFile: String,
                                   availableTime: Long) {
  require(windowDuration > 0 && windowDuration % slideDuration == 0, "window duration must be greater than 0 and multiple of slide duration")
  val numberOfPanes : Int = (windowDuration / slideDuration).toInt
  val frequency: Double = math.floor(slideDuration.toDouble / availableTime)
}

/**
 * Configuration trait for the algorithm, have two specializations the naive one and the algorithm one
 */
sealed trait AlgorithmConfiguration {
  val k: Int
  val alpha: Double
  val maximumQueryCardinalityPercentage: Double

  val timeForQueryComputation: (SimulationConfiguration, Int, Long) => Long
  require(maximumQueryCardinalityPercentage > 0 && maximumQueryCardinalityPercentage <= 1, "percentage of records in query result must be in ]0,1]")

  val name: String = this match {
    case _: NaiveConfiguration => "NAIVE"
    case x: StreamAnalysisConfiguration if x.singleQuery && !x.knapsack => "A-S1"
    case x: StreamAnalysisConfiguration if !x.knapsack => "A-SE"
    case x: StreamAnalysisConfiguration if  x.knapsack => "A-SKE"
  }
}

/**
 * Configuration class for naive execution
 * @param k the number of attributes in the group-by set of the queries that can be executed
 * @param alpha the alpha parameter, for weighting the support in the score
 * @param maximumQueryCardinalityPercentage the maximum cardinality of records ]0,1]
 * @param timeForQueryComputation the time for compute a query given a simulation configuration and the remaining time
 */
case class NaiveConfiguration(k: Int,
                              alpha: Double,
                              maximumQueryCardinalityPercentage: Double,
                              timeForQueryComputation: (SimulationConfiguration, Int, Long) => Long) extends AlgorithmConfiguration

object NaiveConfiguration {
  def apply(k: Int, alpha: Double, maximumQueryCardinalityPercentage: Double = 0.05): NaiveConfiguration = streamanalysis.input.NaiveConfiguration(
    k = k,
    alpha = alpha,
    maximumQueryCardinalityPercentage = maximumQueryCardinalityPercentage,
    timeForQueryComputation = QueryExecutionTimeUtils.getExecutionTime
  )
}

/**
 * Configuration class
 *
 * @param k the number of attributes in the gruop-by set of the queries that can be executed
 * @param alpha the alpha parameter, for weighting the support in the score
 * @param timeForQueryComputation the time for compute a query given a simulation configuration and the remaining time
 * @param knapsack if the knapsack algorithm should be used it is true
 * @param stateCapacity the state capacity, percentage in ]0,1]
 * @param singleQuery the algorithm can execute just a query
 */
case class StreamAnalysisConfiguration(k: Int,
                                       alpha: Double,
                                       timeForQueryComputation: (SimulationConfiguration, Int, Long) => Long, knapsack: Boolean, stateCapacity: Double = 0.05,
                                       singleQuery: Boolean) extends AlgorithmConfiguration {

  def getMaximumOfRecordsToStore(dataSize: Int): Int = {
    math.ceil(dataSize * stateCapacity).toInt
  }

  def getMaximumOfRecordsToStore(data: Seq[Record]): Int = {
    getMaximumOfRecordsToStore(data.size)
  }

  override lazy val maximumQueryCardinalityPercentage: Double = stateCapacity
}

object StreamAnalysisConfiguration {
  /**
   * Get the configurations for the given parameters
   * @param queryExecutionTime the query execution time
   * @param k the alpha parameter, for weighting the support in the score
   * @param alphas the alphas values
   * @param stateCapacities the state capacities percentages
   * @param algorithms the algorithms to consider
   * @return the configurations
   */
  def getConfigurations(queryExecutionTime: (SimulationConfiguration, Int, Long) => Long, k: Int, alphas: Set[Double], stateCapacities: Set[Double],
                        algorithms: Set[OurExecutionAlgorithm]): Seq[StreamAnalysisConfiguration] = {
    alphas.toSeq.flatMap(alpha => {
      stateCapacities.toSeq.flatMap(stateCapacity => algorithms.map(a => {
        val (knapsack, singleQuery) = a match {
          case ASKE => (true, false)
          case ASE => (false, false)
          case AS1 => (false, true)
        }
        StreamAnalysisConfiguration(
          k = k,
          alpha = alpha,
          timeForQueryComputation = queryExecutionTime,
          stateCapacity = stateCapacity,
          knapsack = knapsack,
          singleQuery = singleQuery
        )
      }))
    })
  }
}