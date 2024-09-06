package it.unibo.big.streamanalysis.algorithm.naive

import it.unibo.big.streamanalysis.algorithm.statistics.StatisticsWriter
import it.unibo.big.streamanalysis.algorithm.execution.QueryExecution.executeQuery
import it.unibo.big.streamanalysis.algorithm.generation.QueryUtils.updateQueriesStatistics
import it.unibo.big.streamanalysis.algorithm.generation.choosing.ScoreUtils.{getBestQuery, sortQueries}
import it.unibo.big.streamanalysis.algorithm.state.{QueryStatisticsInPane, State}
import it.unibo.big.streamanalysis.input.GPSJConcepts.GPQuery
import it.unibo.big.streamanalysis.input.RecordModeling.{Record, Window}
import it.unibo.big.streamanalysis.input.SimulationConfiguration
import org.slf4j.{Logger, LoggerFactory}

object NaiveQueriesExecutor {
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * Execute the queries
   * @param simulationConfiguration the simulation configuration
   * @param algorithmState the algorithm state
   * @param data the data in the pane
   * @param window the window
   * @param dimensions the dimensions of the data
   * @param measures the measures of the data
   * @return (if present) the new selected query with the score
   */
  def apply(simulationConfiguration: SimulationConfiguration, algorithmState: State, data: Seq[Record], window: Window,
            dimensions: Set[String], measures: Set[String]): Option[(GPQuery, QueryStatisticsInPane)] = {
    val previousSelectedQuery = algorithmState.update(window)
    val startTime = System.currentTimeMillis()
    val configuration = algorithmState.configuration
    updateQueriesStatistics(data, algorithmState, previousSelectedQuery, window.paneTime, dimensions, measures, simulationConfiguration)
    val queriesWithScore = algorithmState.getQueries(window.paneTime).filter(_._2.exists)
    LOGGER.info(s"[NAIVE EXECUTING] Executing queries: ${queriesWithScore.size}), input query: ${previousSelectedQuery.map(_.dimensions.mkString(","))}")
    var availableTime = simulationConfiguration.availableTime - (startTime - System.currentTimeMillis())
    val numberOfQueriesToExecute = math.max(math.floor(availableTime / configuration.timeForQueryComputation(simulationConfiguration, configuration.k, availableTime)).toInt, 0)
    var executedQueriesConsideringTimeConstraint = 0

    queriesWithScore.foreach{
      case (q, qs) =>
        LOGGER.debug(s"[NAIVE EXECUTING] Executing query ${q.dimensions}")
        val startExecuteQueryTime = System.currentTimeMillis()
        executeQuery(algorithmState, data, q, window.paneTime, numberOfRecords = 0L, maxNumberOfRecords = Int.MaxValue)
        availableTime -= System.currentTimeMillis() - startExecuteQueryTime
        if(availableTime >= 0) {
          executedQueriesConsideringTimeConstraint += 1 //update the number of executed queries
        }
        // change the total score for the query using the real support
        qs.changeTotalScoreForNaive(algorithmState)
    }

    val timeForChooseQuery = System.currentTimeMillis()
    val firstExecutedQuery = sortQueries(queriesWithScore).headOption
    val selectedQuery = getBestQuery(window.paneTime, algorithmState, previousSelectedQuery, firstExecutedQuery)
    algorithmState.getTimeStatistics.addTimeForChooseQueries(System.currentTimeMillis() - timeForChooseQuery)
    val totalTime =  System.currentTimeMillis() - startTime
    StatisticsWriter.writeStatistics(simulationConfiguration, algorithmState, selectedQuery.map(_._1), window, totalTime, previousSelectedQuery, data, math.max(executedQueriesConsideringTimeConstraint, numberOfQueriesToExecute))

    if(selectedQuery.nonEmpty) {
      algorithmState.compute(window, selectedQuery.get._1)
    }
    algorithmState.getTimeStatistics.reset()
    selectedQuery
  }
}
