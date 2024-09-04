package it.unibo.big.query.app

import it.unibo.big.input.{ConfigurationSetting, NaiveConfiguration, StreamAnalysisConfiguration}
import it.unibo.big.query.app.DatasetsUtils.Dataset
import it.unibo.big.query.app.common.WindowedQueryExecution.simulate
import it.unibo.big.query.state.StateUtils.OwnState
import org.slf4j.{Logger, LoggerFactory}

object ExecuteTests {

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
   * @param configurations the settings with respective naive (if present) and stream analysis configurations
   * @param numberOfWindowsToConsider the number of windows to consider
   * @param numberOfPanes the number of panes in the window
   * @param slideDuration the slide duration (number of records to consider)
   * @param datasets the datasets to consider
   * @param availableTime the available time for the execution
   */
  def apply(configurations: Dataset => Map[ConfigurationSetting, (Option[NaiveConfiguration], Seq[StreamAnalysisConfiguration])],
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
