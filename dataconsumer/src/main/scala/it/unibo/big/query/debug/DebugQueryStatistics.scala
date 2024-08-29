package it.unibo.big.query.debug

import it.unibo.big.input.GPSJConcepts.GPQuery
import it.unibo.big.input.RecordModeling.Window
import it.unibo.big.input.{ConfigurationUtils, NaiveConfiguration, SimulationConfiguration}
import it.unibo.big.query.debug.DebugUtils.{DebugStatistics, formattedString, getValue}
import it.unibo.big.query.similarity.QuerySimilarity
import it.unibo.big.query.state.{QueryStatisticsInPane, State}
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
case class DebugQueryStatistics(window: Window,
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
  private val previousQuerySimilarity = if(selectedQuery.contains(query) && previousChosenQuery.nonEmpty) QuerySimilarity.compute(previousChosenQuery.get.dimensions, query.dimensions) else null
  private val timeStatistics = state.getTimeStatistics
  private val numberOfPanes = state.numberOfPanes

  private val maxNumberOfRecords = stats.map {
    case (t, stat) => t -> getValue(state, c => ConfigurationUtils.getMaximumNumberOfRecordsToStore(stat.flatMap(_.inputRecords).getOrElse(0), c.logFactor))
  }.toSeq.sortBy(_._1)

  private val SOPSupport = lastPaneStatistics.score.supportValue
  private val SOPFd = lastPaneStatistics.score.fdValue
  if(math.abs((algorithmConfiguration.alpha * SOPSupport + algorithmConfiguration.beta * SOPFd) - lastPaneStatistics.score.value) > 0.001) {
    LOGGER.warn(s"Score ${lastPaneStatistics.score.value} is not equal to ${algorithmConfiguration.alpha} * $SOPFd + ${algorithmConfiguration.beta} * $SOPSupport")
  }

  private val lastPaneMaxNumberOfRecords = util.Try(maxNumberOfRecords.last._2.get.asInstanceOf[Any]).toOption

  override val data: Seq[(String, Any)] = super.data ++ Map(
    "selected" -> selectedQuery.contains(query),
    "executed" -> lastPaneStatistics.isExecuted,
    "stored" -> lastPaneStatistics.isStored,
    "notChange" -> (selectedQuery.contains(query) && previousChosenQuery == selectedQuery),
    "SOP" -> lastPaneStatistics.score.value,
    "SOPSupport" -> SOPSupport,
    "SOPFd" -> SOPFd,
    "percentageOfExecutedPanes" -> queryExecuted.count(_ == true).toDouble / numberOfPanes * 100,
    "percentageOfStoredPanes" -> queryStored.count(_ == true).toDouble / numberOfPanes * 100,
    "executedPanes" -> formattedString(queryExecuted),
    "storedPanes" -> formattedString(queryStored),
    "querySupportLastPaneEstimated" -> lastPaneStatistics.score.paneSupport,
    "querySupportLastPaneReal" -> lastPaneStatistics.realSupport,
    "estimatedNumberOfRecordsPanes" -> formattedString(sortedStatistics.map(_._2.map(_.estimatedNumberOfRecords).getOrElse(0D))),
    "executionTimePanes" -> formattedString(sortedStatistics.map(_._2.flatMap(_.getExecutionTime).getOrElse(0D))),
    "numberOfRecordsPanes" -> formattedString(sortedStatistics.map(_._2.flatMap(_.inputRecords).getOrElse(0D))),
    "lastPaneEstRecords" -> lastPaneStatistics.estimatedNumberOfRecords,
    "lastPaneRealRecords" -> lastPaneStatistics.realNumberOfRecords.getOrElse(0),
    "lastPaneExecutionTime" -> lastPaneStatistics.getExecutionTime.getOrElse(0D),
    "lastPaneRecords" -> lastPaneStatistics.inputRecords.getOrElse(0),
    "panesMaxRecords" -> formattedString(maxNumberOfRecords.map(x => x._2.getOrElse(Int.MaxValue))),
    "lastPaneMaxRecords" -> lastPaneMaxNumberOfRecords.orNull,
    "totalTime" -> totalTime,
    "isNaive" ->  algorithmConfiguration.isInstanceOf[NaiveConfiguration],
    "timeForUpdateWindow" -> timeStatistics.getTimeForUpdateWindow,
    "timeForComputeQueryInTheWindow" -> timeStatistics.getTimeForComputeQueryInTheWindow,
    "timeForGettingScores" -> timeStatistics.getTimeForGettingScores,
    "timeForUpdateThePane" -> timeStatistics.getTimeForUpdateThePane,
    "timeForScoreComputation" -> timeStatistics.getTimeForScoreComputation,
    "timeForChooseQueries" -> timeStatistics.getTimeForChooseQueries,
    "timeForQueryExecution" -> timeStatistics.getTimeForQueryExecution,
    "previousQuerySimilarity" -> previousQuerySimilarity,
    "queriesInPane" -> state.getQueriesInPane(window.paneTime).size,
    "measures" -> query.measures.size,
    "numberOfAttributes" -> numberOfAttributes,
    "availableTime" -> simulationConfiguration.availableTime,
    "frequency" -> simulationConfiguration.frequency,
    "estimatedTime" -> lastPaneStatistics.estimatedTime
  ).toSeq.sortBy(_._1)
}