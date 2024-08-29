package it.unibo.big.query.app

import it.unibo.big.input
import it.unibo.big.input.DataDefinition.{NullData, NumericData, StringData}
import it.unibo.big.input.GPSJConcepts.QueryPattern
import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.input.UserPreferences.parseUserPreferences
import it.unibo.big.input.{ConfigurationUtils, SimulationConfiguration}
import it.unibo.big.kafka.KafkaConfiguration
import it.unibo.big.kafka.consumer.KafkaDataSource
import it.unibo.big.kafka.producer.KafkaProducerResult
import it.unibo.big.query.algorithm.QueryUpdate
import it.unibo.big.query.app.DatasetsUtils.Dataset
import it.unibo.big.query.app.common.WindowedQueryExecution.getDimensionsSupport
import it.unibo.big.query.execution.QueryExecutionTimeUtils
import it.unibo.big.query.state.{State, StateUtils}
import it.unibo.big.window.DataWindow.windowing
import org.slf4j.{Logger, LoggerFactory}

object SingleAlgorithmExecution {
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  @volatile private var running: Boolean = false
  private var producer : KafkaProducerResult = _
  private var source : KafkaDataSource = _

  /**
   * Execute the simulation
   * @param windowDuration the duration of the window
   * @param slideDuration the duration of the slide
   * @param numberOfAttributesGroupBySet the number of attributes in the group by set
   * @param alpha the alpha value
   * @param percentageOfRecordsInState the percentage of records in the state
   * @param kafkaConfiguration the kafka configuration
   */
  def executeSimulation(windowDuration: Long, slideDuration: Long, numberOfAttributesGroupBySet: Int, alpha: Double, percentageOfRecordsInState: Double, kafkaConfiguration: KafkaConfiguration): Unit = {
    println("Executing simulation")
    running = true
    source = new KafkaDataSource(kafkaConfiguration)
    producer = new KafkaProducerResult(kafkaConfiguration)
    val userPreferencesFile = "debug/analysis/user_preferences.csv"
    var userPreferences = parseUserPreferences(userPreferencesFile)
    var state: Option[State] = None
    var simulationConfiguration: Option[SimulationConfiguration] = None

    windowing[Record](source, windowDuration, slideDuration, (window, data, _) => {
      if (!running) {
        LOGGER.info("Simulation stopped")
        return
      }
      val newPaneData = data._2
      val windowData = data._1
      val previousPaneData = data._3
      if (state.isEmpty && simulationConfiguration.isEmpty && newPaneData.nonEmpty) {
        val logFactor = ConfigurationUtils.getLogFactor(percentageOfRecordsInState, newPaneData.size)
        state = Some(State(Map(), input.StreamAnalysisConfiguration(pattern = QueryPattern(numberOfAttributesGroupBySet, None),
          alpha = alpha,
          timeForQueryComputation = QueryExecutionTimeUtils.getExecutionTime,
          logFactor = logFactor,
          knapsack = Some(1),
          percentageOfRecordsInQueryResults = percentageOfRecordsInState,
          singleQuery = false)))

        val dataset: Dataset = newPaneData.head.dataset.get
        val path = dataset.path(StateUtils.OwnState)
        simulationConfiguration = Some(SimulationConfiguration(dataset,
          windowDuration = windowDuration,
          slideDuration = slideDuration,
          numberOfWindowsToConsider = None,
          statisticsFile = s"${path}stats.csv",
          chosenQueryStatisticsFile = s"${path}stats_chosen_query.csv",
          datasetStatisticsFile = s"${path}stats_dataset.csv",
          userPreferencesFile = userPreferencesFile, availableTime = slideDuration))
      }
      if (state.nonEmpty) {
        //consider the user preferences defined in the previous iteration
        val paneUserPreferences = userPreferences

        //info application statistics
        val dataDimensions = getDimensionsSupport(newPaneData)
        val dimensions = dataDimensions.keySet
        val measures = newPaneData.flatMap(_.measures).toSet.diff(dimensions)

        //The user can change the dimensions of interest by excluding or including some of them
        userPreferences = parseUserPreferences(simulationConfiguration.get.userPreferencesFile)

        dataDimensions.foreach { case (d, s) => LOGGER.info(s"Dimension $d with support $s") }

        val (newQuery, _) = QueryUpdate.updateQuery(simulationConfiguration.get, state.get, newPaneData, window, paneUserPreferences, dimensions, measures, writeDebug = false)
        if (newQuery.isDefined) {
          val (result, queryPanesSupport) = state.get.compute(window, newQuery.get)
          LOGGER.info(s"[QUERY ${newQuery.get.dimensions}] $queryPanesSupport % panes with query config = ${state.get.configuration}")
          LOGGER.info(s"Query ${newQuery.get}")
          val keys = newQuery.get.dimensions.toList ++ newQuery.get.aggregations.map(_.toString).toList
          LOGGER.debug(keys.mkString("|"))
          val resultRecords = result.map(x => keys.map(k => x.getOrElse(k, NullData)).map { case NumericData(value) => value
              case StringData(value) => value
              case NullData => null
          })
          producer.sendMessage(keys, resultRecords, dataDimensions, newPaneData.size, window.paneTime)
        }
      }
    }, None, None, numberOfWindowsToConsider = None)
  }

  def stopSimulation(): Unit = {
    running = false
    source.close()
    producer.close()
  }
}
