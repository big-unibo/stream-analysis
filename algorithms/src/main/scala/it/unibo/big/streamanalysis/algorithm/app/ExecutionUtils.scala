package it.unibo.big.streamanalysis.algorithm.app

import it.unibo.big.streamanalysis.algorithm.app.ExecutionUtils.{ExecutionAlgorithm, OurExecutionAlgorithm}
import it.unibo.big.streamanalysis.algorithm.app.common.WindowedQueryExecution.simulate
import it.unibo.big.streamanalysis.algorithm.execution.QueryExecutionTimeUtils
import it.unibo.big.streamanalysis.algorithm.state.StateUtils.OwnState
import it.unibo.big.streamanalysis.input.GPSJConcepts.QueryPattern
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

    val configurations: Map[ConfigurationSetting, (Option[NaiveConfiguration], Seq[StreamAnalysisConfiguration])] = executionConfigurations.map{
      case ExecutionConfiguration(alpha, groupBySet, maxRecordsStatePercentage, maxRecordsQueries, algorithms) =>
        val logFactors = maxRecordsStatePercentage.toSeq.map(p => {
          p -> ConfigurationUtils.getLogFactor(p, numberOfRecordsPane.toInt)
        }).toMap
        val ourExecutionAlgorithms = algorithms.collect{
          case x if x.isInstanceOf[OurExecutionAlgorithm] => x.asInstanceOf[OurExecutionAlgorithm]
        }
        val qp = QueryPattern(groupBySet, None)
        val usedLogFactors = logFactors.filterKeys(_ >= maxRecordsQueries).values.toSet
        if(ourExecutionAlgorithms.nonEmpty) {
          require(usedLogFactors.nonEmpty, s"Log factor not found for $maxRecordsQueries")
        }
        val setting = ConfigurationSetting(alpha, qp, maxRecordsQueries)
        val naiveConfiguration = if(algorithms.contains(Naive)) Some(NaiveConfiguration(qp, alpha, maxRecordsQueries)) else None
        val algorithmsConfigurations = if(ourExecutionAlgorithms.nonEmpty) StreamAnalysisConfiguration.getConfigurations(
          queryExecutionTime = queryExecutionTime,
          queryPattern = qp,
          alphas = Set(alpha),
          logFactors = usedLogFactors,
          algorithms = ourExecutionAlgorithms,
          getPercentage = _ => maxRecordsQueries
        ) else Seq()
        setting -> (naiveConfiguration, algorithmsConfigurations)
    }.groupBy(_._1).mapValues(xs => (util.Try(xs.map(_._2._1).filter(_.nonEmpty).head.get).toOption, xs.flatMap(_._2._2)))
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
   * @param configurations the settings with respective naive (if present) and stream streamanalysis configurations
   * @param numberOfWindowsToConsider the number of windows to consider
   * @param numberOfPanes the number of panes in the window
   * @param slideDuration the slide duration (number of records to consider)
   * @param datasets the datasets to consider
   * @param availableTime the available time for the execution
   */
  private def execute(configurations: Dataset => Map[ConfigurationSetting, (Option[NaiveConfiguration], Seq[StreamAnalysisConfiguration])],
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
 * @param groupBySet  the group by set of queries
 * @param maxRecordsStatePercentage the max records state percentage
 * @param maxRecordsQueries the max records queries
 * @param algorithms to execute
 */
case class ExecutionConfiguration(alpha: Double, groupBySet: Int, maxRecordsStatePercentage: Set[Double], maxRecordsQueries: Double,
                                  algorithms: Set[ExecutionAlgorithm]) {
  require(alpha >= 0 && alpha <= 1, "Alpha must be in [0, 1]")
  require(groupBySet > 0, "Group by set must be positive")
  if(maxRecordsStatePercentage.isEmpty) {
    require(!algorithms.exists(_.isInstanceOf[OurExecutionAlgorithm]), "If no max records state percentage is defined, only naive algorithm must be executed")
  }
  require(maxRecordsQueries >= 0, "Max records queries must be positive")
  require(maxRecordsStatePercentage.forall(x => x > 0 && x <= 1), "Max records state percentage must be between ]0,1]")
  require(maxRecordsStatePercentage.forall(_ >= maxRecordsQueries), "Max records state percentage must be greater than max records queries")
  require(algorithms.nonEmpty, "At least one algorithm must be executed")
}


