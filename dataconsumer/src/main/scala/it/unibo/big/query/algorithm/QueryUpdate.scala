package it.unibo.big.query.algorithm

import it.unibo.big.input.GPSJConcepts.GPQuery
import it.unibo.big.input.RecordModeling.{Record, Window}
import it.unibo.big.input.SimulationConfiguration
import it.unibo.big.input.UserPreferences.UserPreferences
import it.unibo.big.query.execution.QueryExecution._
import it.unibo.big.query.generation.QueryUtils.updateQueriesStatistics
import it.unibo.big.query.state.State

object QueryUpdate {

  /**
   * Update the current query considering the current pane and the data and the state
   * @param simulationConfiguration the simulation configuration
   * @param algorithmState the algorithm state
   * @param data the data
   * @param window the window
   * @param userPreferences the user preferences
   * @param dimensions the dimensions of the data
   * @param measures the measures of the data
   * @param writeDebug if true write debug information
   * @return the actual query executed and the max number of queries to execute
   */
  def updateQuery(simulationConfiguration: SimulationConfiguration, algorithmState: State, data: Seq[Record],
                  window: Window, userPreferences: UserPreferences, dimensions: Set[String], measures: Set[String], writeDebug: Boolean): (Option[GPQuery], Int) = {
    val startTime = System.currentTimeMillis()
    val previousSelectedQuery = algorithmState.update(window)
    //update the statistics of the queries in the state considering the current pane and the data
    updateQueriesStatistics(data, algorithmState, previousSelectedQuery, userPreferences, window.paneTime, dimensions, measures, simulationConfiguration)
    val queryStatisticsTime = System.currentTimeMillis() - startTime
    //add time statistics for score calculation
    executeQueries(simulationConfiguration, algorithmState, window, data, previousSelectedQuery, startTime, queryStatisticsTime, writeDebug)
  }
}