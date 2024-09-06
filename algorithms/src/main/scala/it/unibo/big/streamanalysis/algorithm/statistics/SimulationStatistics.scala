package it.unibo.big.streamanalysis.algorithm.statistics

import it.unibo.big.streamanalysis.algorithm.statistics.StatisticsUtils.{DebugStatistics, formattedString, getValue}
import it.unibo.big.streamanalysis.algorithm.state.{QueryStatisticsInPane, State}
import it.unibo.big.streamanalysis.input.GPSJConcepts.GPQuery
import it.unibo.big.streamanalysis.input.RecordModeling.Window
import it.unibo.big.streamanalysis.input.{NaiveConfiguration, SimulationConfiguration}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Debug statistics for the query
 * @param window the window
 * @param simulationConfiguration the simulation configuration
 * @param query the query
 * @param previousChosenQuery the previous chosen query
 * @param selectedQuery the selected query at paneTime
 * @param state the algorithm state
 * @param totalTime the total time for the execution of the algorithm in the pane, not just the query
 * @param numberOfAttributes the number of attributes in the data
 * @param numberOfQueriesToExecute the number of queries to execute
 */
case class SimulationStatistics(window: Window,
                                simulationConfiguration: SimulationConfiguration,
                                query: GPQuery, previousChosenQuery: Option[GPQuery], selectedQuery: Option[GPQuery],
                                state: State, totalTime: Long, numberOfAttributes: Int, numberOfQueriesToExecute: Int) extends DebugStatistics {

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  private val stats = state.getQueryStatistics(query)
  private val sortedStatistics: Seq[(Long, Option[QueryStatisticsInPane])] = stats.toSeq.sortBy(_._1)

  private val lastPaneStatistics = sortedStatistics.lastOption.get._2.get
  private val queryExecuted = sortedStatistics.map(x => x._2.exists(_.isExecuted == true))
  private val queryStored = sortedStatistics.map(x => x._2.exists(_.isStored == true))
  //If q is the selected query, calculate the similarity between q and the previous query
  private val timeStatistics = state.getTimeStatistics

  if(math.abs((algorithmConfiguration.alpha * lastPaneStatistics.score.supportValue + (1 - algorithmConfiguration.alpha) * lastPaneStatistics.score.similarityValue) - lastPaneStatistics.score.value) > 0.001) {
    LOGGER.warn(s"Score ${lastPaneStatistics.score.value} is not equal to ${algorithmConfiguration.alpha} * ${lastPaneStatistics.score.supportValue} + ${1 - algorithmConfiguration.alpha} * ${lastPaneStatistics.score.similarityValue}")
  }

  private val lastPaneMaxNumberOfRecords = lastPaneStatistics.inputRecords.map(x => getValue(state, _.getMaximumOfRecordsToStore(x))).getOrElse(0)

  override val data: Seq[(String, Any)] = super.data ++ Map(
    "selected" -> selectedQuery.contains(query),
    "executed" -> lastPaneStatistics.isExecuted,
    "stored" -> lastPaneStatistics.isStored,
    "score" -> lastPaneStatistics.score.value,
    "support" -> lastPaneStatistics.score.supportValue,
    "similarity" -> lastPaneStatistics.score.similarityValue,
    "supportLastPaneEstimated" -> lastPaneStatistics.score.paneSupport,
    "supportLastPaneReal" -> lastPaneStatistics.realSupport,
    "queryCardinalityLastPane" -> lastPaneStatistics.realNumberOfRecords.getOrElse(0),
    "queryExecutionTime" -> lastPaneStatistics.getExecutionTime.getOrElse(0D),
    "queryEstimatedTime" -> lastPaneStatistics.estimatedTime,
    "lastPaneRecords" -> lastPaneStatistics.inputRecords.getOrElse(0),
    "lastPaneMaxRecords" -> lastPaneMaxNumberOfRecords,
    "totalTime" -> totalTime,
    "isNaive" ->  algorithmConfiguration.isInstanceOf[NaiveConfiguration],
    "timeForUpdateWindow" -> timeStatistics.getTimeForUpdateWindow,
    "timeForComputeQueryInTheWindow" -> timeStatistics.getTimeForComputeQueryInTheWindow,
    "timeForGettingScores" -> timeStatistics.getTimeForGettingScores,
    "timeForUpdateThePane" -> timeStatistics.getTimeForUpdateThePane,
    "timeForScoreComputation" -> timeStatistics.getTimeForScoreComputation,
    "timeForChooseQueries" -> timeStatistics.getTimeForChooseQueries,
    "timeForQueryExecution" -> timeStatistics.getTimeForQueryExecution,
    "feasibleQueries" -> state.getQueriesInPane(window.paneTime).size,
    "measures" -> query.measures.size,
    "numberOfAttributes" -> numberOfAttributes,
    "availableTime" -> simulationConfiguration.availableTime,
    "frequency" -> simulationConfiguration.frequency
  ).toSeq.sortBy(_._1)
}