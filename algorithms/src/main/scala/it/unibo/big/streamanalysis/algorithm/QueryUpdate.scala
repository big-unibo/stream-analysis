package it.unibo.big.streamanalysis.algorithm

import it.unibo.big.streamanalysis.algorithm.execution.QueryExecution.executeQueries
import it.unibo.big.streamanalysis.algorithm.generation.QueryUtils.updateQueriesStatistics
import it.unibo.big.streamanalysis.algorithm.state.State
import it.unibo.big.streamanalysis.input.GPSJConcepts.GPQuery
import it.unibo.big.streamanalysis.input.RecordModeling.{Record, Window}
import it.unibo.big.streamanalysis.input.SimulationConfiguration

object QueryUpdate {

  /**
   * Update the current query considering the current pane and the data and the state
   * @param simulationConfiguration the simulation configuration
   * @param algorithmState the algorithm state
   * @param data the data
   * @param window the window
   * @param dimensions the dimensions of the data
   * @param measures the measures of the data
   * @param writeDebug if true write debug information
   * @return the actual query executed and the max number of queries to execute
   */
  def updateQuery(simulationConfiguration: SimulationConfiguration, algorithmState: State, data: Seq[Record],
                  window: Window, dimensions: Set[String], measures: Set[String], writeDebug: Boolean): (Option[GPQuery], Int) = {
    val startTime = System.currentTimeMillis()
    val previousSelectedQuery = algorithmState.update(window)
    //update the statistics of the queries in the state considering the current pane and the data
    updateQueriesStatistics(data, algorithmState, previousSelectedQuery, window.paneTime, dimensions, measures, simulationConfiguration)
    val queryStatisticsTime = System.currentTimeMillis() - startTime
    //add time statistics for score calculation
    executeQueries(simulationConfiguration, algorithmState, window, data, previousSelectedQuery, startTime, queryStatisticsTime, writeDebug)
  }
}