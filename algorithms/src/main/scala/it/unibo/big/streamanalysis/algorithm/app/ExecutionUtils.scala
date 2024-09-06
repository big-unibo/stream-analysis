package it.unibo.big.streamanalysis.algorithm.app

import it.unibo.big.streamanalysis.algorithm.app.ExecutionUtils.{ExecutionAlgorithm, OurExecutionAlgorithm}
import it.unibo.big.streamanalysis.algorithm.app.common.WindowedQueryExecution.simulate
import it.unibo.big.streamanalysis.algorithm.execution.QueryExecutionTimeUtils
import it.unibo.big.streamanalysis.algorithm.state.StateUtils.OwnState
import it.unibo.big.streamanalysis.input._
import it.unibo.big.streamanalysis.utils.DatasetsUtils.{ChangingSyntheticDataset, Dataset}
import org.slf4j.{Logger, LoggerFactory}

object ExecutionUtils {

  /**
   * Apply the full datasets test paper
   * @param executionConfigurations the execution configurations
   * @param datasets the datasets to execute the tests
   * @param numberOfRecordsPane the number of records per pane
   * @param numberOfPanes the number of panes
   * @param numberOfWindowsToConsider the number of windows to consider
   * @param queryExecutionTime the query execution time
   * @param frequency the frequency of records in ms
   */
  def apply(executionConfigurations: Seq[ExecutionConfiguration], datasets: Seq[Dataset], numberOfRecordsPane: Long = 10000L, numberOfPanes: Int = 5,
            queryExecutionTime : (SimulationConfiguration, Int, Long) => Long = QueryExecutionTimeUtils.getExecutionTime,
            numberOfWindowsToConsider: Dataset => Int = d => if(d.isInstanceOf[ChangingSyntheticDataset]) 30 else 15, frequency: Double = 10): Unit = {

    require(executionConfigurations.nonEmpty, "Execution configurations must be non empty")

    val configurations: Map[ExecutionConfiguration, (Option[NaiveConfiguration], Seq[StreamAnalysisConfiguration])] = executionConfigurations.map(conf => {
        val ourExecutionAlgorithms = conf.algorithms.collect{
          case x if x.isInstanceOf[OurExecutionAlgorithm] => x.asInstanceOf[OurExecutionAlgorithm]
        }
        val naiveConfiguration = if(conf.algorithms.contains(Naive)) Some(NaiveConfiguration(conf.k, conf.alpha, conf.maximumQueryCardinalityPercentage)) else None
        val algorithmsConfigurations = if(ourExecutionAlgorithms.nonEmpty) StreamAnalysisConfiguration.getConfigurations(
          queryExecutionTime = queryExecutionTime,
          k = conf.k,
          alphas = Set(conf.alpha),
          stateCapacities = conf.stateCapacities,
          algorithms = ourExecutionAlgorithms
        ) else Seq()
        conf -> (naiveConfiguration, algorithmsConfigurations)
    }).groupBy(_._1).mapValues(xs => (util.Try(xs.map(_._2._1).filter(_.nonEmpty).head.get).toOption, xs.flatMap(_._2._2)))
    execute(_ => configurations, numberOfWindowsToConsider, numberOfPanes, slideDuration = numberOfRecordsPane, datasets = datasets, availableTime = math.ceil(numberOfRecordsPane / frequency).toLong)
  }

  /**
   * Execution algorithm
   */
  sealed trait ExecutionAlgorithm

  sealed trait OurExecutionAlgorithm extends ExecutionAlgorithm

  /**
   * Execution algorithm naive
   */
  case object Naive extends ExecutionAlgorithm

  /**
   * Execution algorithm ASKE, with knapsack
   */
  case object ASKE extends OurExecutionAlgorithm

  /**
   * Execution algorithm ASE, without knapsack
   */
  case object ASE extends OurExecutionAlgorithm

  /**
   * Execution algorithm AS1, with one single query
   */
  case object AS1 extends OurExecutionAlgorithm

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * Execute the algorithms in the scenarios datasets
   *
   * @param configurations the settings with respective naive (if present) and other algorithms' configurations
   * @param numberOfWindowsToConsider the number of windows to consider
   * @param numberOfPanes the number of panes in the window
   * @param slideDuration the slide duration (number of records to consider)
   * @param datasets the datasets to consider
   * @param availableTime the available time for the execution
   */
  private def execute(configurations: Dataset => Map[ExecutionConfiguration, (Option[NaiveConfiguration], Seq[StreamAnalysisConfiguration])],
            numberOfWindowsToConsider: Dataset => Int, numberOfPanes: Int, slideDuration: Long, datasets: Seq[Dataset], availableTime: Long): Unit = {
    datasets.foreach(dataset => {

      val stateTypes = Seq(OwnState)

      stateTypes.foreach(state => {
        LOGGER.info(s"Reading ${dataset.fileName}")
        simulate(
          dataset = dataset,
          windowDuration = numberOfPanes * slideDuration,
          slideDuration = slideDuration,
          numberOfWindowsToConsider = numberOfWindowsToConsider(dataset),
          configurations = configurations(dataset),
          stateType = state,
          availableTime = availableTime
        )
      })
    })
  }
}

/**
 * Execution configuration
 * @param alpha the alpha
 * @param k  the number of attributes in the group-by set of the queries that can be executed
 * @param stateCapacities the state capacities of execution, percentage in ]0,1]
 * @param maximumQueryCardinalityPercentage the maximum query cardinality percentage ]0,1]
 * @param algorithms to execute
 */
case class ExecutionConfiguration(alpha: Double, k: Int, stateCapacities: Set[Double], maximumQueryCardinalityPercentage: Double,
                                  algorithms: Set[ExecutionAlgorithm]) {
  require(alpha >= 0 && alpha <= 1, "Alpha must be in [0, 1]")
  require(k > 0, "Group by set must be positive")
  if(stateCapacities.isEmpty) {
    require(!algorithms.exists(_.isInstanceOf[OurExecutionAlgorithm]), "If no state capacity limit is defined, only naive algorithm must be executed")
  }
  require(maximumQueryCardinalityPercentage >= 0, "Maximum query cardinality must be positive")
  require(stateCapacities.forall(x => x > 0 && x <= 1), "State capacities must be between ]0,1]")
  require(stateCapacities.forall(_ >= maximumQueryCardinalityPercentage), "State capacities must be greater equals than maximum query cardinality")
  require(algorithms.nonEmpty, "At least one algorithm must be executed")

  /**
   * Check if the configuration is compliant with the given algorithm configuration
   * @param algorithmConfiguration the algorithm configuration
   */
  def isCompliantConfiguration(algorithmConfiguration: AlgorithmConfiguration): Unit = {
    require(alpha == algorithmConfiguration.alpha, "alpha must be the same")
    require(k == algorithmConfiguration.k, "query k must be the same")
    require(maximumQueryCardinalityPercentage == algorithmConfiguration.maximumQueryCardinalityPercentage, "Maximum percentage of records in query result must be the same")
  }
}

/**
 * Companion object of ExecutionConfiguration
 */
object ExecutionConfiguration {
  def apply(alpha: Double, k: Int, stateCapacities: Set[Double], algorithms: Set[ExecutionAlgorithm]): ExecutionConfiguration = {
    ExecutionConfiguration(alpha, k, stateCapacities, stateCapacities.min, algorithms)
  }

  def apply(alpha: Double, groupBySet: Int, maximumQueryCardinality: Double, algorithms: Set[ExecutionAlgorithm]): ExecutionConfiguration = {
    new ExecutionConfiguration(alpha, groupBySet, Set(), maximumQueryCardinality, algorithms)
  }
}


