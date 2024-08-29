package it.unibo.big.query.algorithm.naive

import it.unibo.big.input.GPSJConcepts.GPQuery
import it.unibo.big.input.RecordModeling.{Record, Window}
import it.unibo.big.input.SimulationConfiguration
import it.unibo.big.query.debug.DebugWriter
import it.unibo.big.query.execution.QueryExecution.executeQuery
import it.unibo.big.query.generation.QueryUtils._
import it.unibo.big.query.generation.choosing.ScoreUtils.{getBestQuery, sortQueries}
import it.unibo.big.query.state.{QueryStatisticsInPane, State}
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
    val queriesWithSOP = algorithmState.getQueries(window.paneTime).filter(_._2.exists)
    LOGGER.info(s"[NAIVE EXECUTING] Executing queries: ${queriesWithSOP.size}), input query: ${previousSelectedQuery.map(_.dimensions.mkString(","))}")
    var availableTime = simulationConfiguration.availableTime - (startTime - System.currentTimeMillis())
    val numberOfQueriesToExecute = math.max(math.floor(availableTime / configuration.timeForQueryComputation(simulationConfiguration, configuration.pattern.numberOfDimensions, availableTime)).toInt, 0)
    var executedQueriesConsideringTimeConstraint = 0

    queriesWithSOP.foreach{
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
    val firstExecutedQuery = sortQueries(queriesWithSOP).headOption
    val selectedQuery = getBestQuery(window.paneTime, algorithmState, previousSelectedQuery, firstExecutedQuery)
    algorithmState.getTimeStatistics.addTimeForChooseQueries(System.currentTimeMillis() - timeForChooseQuery)
    val totalTime =  System.currentTimeMillis() - startTime
    DebugWriter.writeStatistics(simulationConfiguration, algorithmState, selectedQuery.map(_._1), window, totalTime, previousSelectedQuery, data, math.max(executedQueriesConsideringTimeConstraint, numberOfQueriesToExecute))

    if(selectedQuery.nonEmpty) {
      algorithmState.compute(window, selectedQuery.get._1)
    }
    algorithmState.getTimeStatistics.reset()
    selectedQuery
  }
}
