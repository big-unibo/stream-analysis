package it.unibo.big.query.debug

import it.unibo.big.input.GPSJConcepts.GPQuery
import it.unibo.big.input.RecordModeling.Window
import it.unibo.big.input.{ConfigurationUtils, SimulationConfiguration}
import it.unibo.big.query.debug.DebugUtils.{DebugStatistics, formattedString, getValue}
import it.unibo.big.query.debug.DebugWriter.LastPaneStatistics
import it.unibo.big.query.generation.choosing.ScoreUtils.Score
import it.unibo.big.query.generation.choosing.ScoreUtils.Score.getWeightedValuesForLastPane
import it.unibo.big.query.state.{QueryStatisticsInPane, State}

/**
 * Statistics for the chosen query
 *
 * @param window the window
 * @param state the algorithm state
 * @param gfNaive the naive algorithm state
 * @param naiveQuery the query chosen from the naive algorithm with its score
 * @param query the query chosen from the current algorithm
 * @param simulationConfiguration the simulation configuration
 * @param lastPaneStatistics the count distinct map
 */
case class ChosenQueryStatistics(window: Window,
                                 state: State,
                                 gfNaive: State, naiveQuery: (GPQuery, QueryStatisticsInPane), query: GPQuery,
                                 simulationConfiguration: SimulationConfiguration, lastPaneStatistics: LastPaneStatistics,
                                  numberOfQueriesToExecute: Int
                                ) extends DebugStatistics {

  //calculate the difference between the two queries
  private val maxNumberOfRecords: Option[Int] = getValue(state, x => ConfigurationUtils.getMaximumNumberOfRecordsToStore(lastPaneStatistics._1, x.logFactor))
  //if are the same query the score is 1
  private val scoreOverNaive = Score(state, Some(naiveQuery._1), window.paneTime, query, None, state.getQueryCardinalities, Map())

  private val scoreOverPreviousForNaive = naiveQuery._2.score
  // calculate the score difference in percentage
  private val representativesOfNewQuery = state.getQueryRepresentativeness(query)
  private val representativesOfNaiveQuery = state.getQueryRepresentativeness(naiveQuery._1)
  private val naiveExecutorRepresentativeness = gfNaive.getQueryRepresentativeness(naiveQuery._1)
  private val naiveSupport = getWeightedValuesForLastPane(gfNaive.getSupports(naiveQuery._1, None), window.paneTime)
  //convert support with respect to naive
  private val suppWrtNaive = scoreOverNaive.supportValue / naiveSupport

  override val data: Seq[(String, Any)] = super.data ++ Map(
    "maximumNumberOfRecords" -> maxNumberOfRecords.getOrElse(0),
    "naiveQuery" -> formattedString(naiveQuery._1.dimensions.toSeq.sorted),
    "SON" -> scoreOverNaive.value,
    "SONSupport" -> scoreOverNaive.supportValue,
    "SONFd" -> scoreOverNaive.fdValue,
    "SOPForNaive" -> scoreOverPreviousForNaive,
    "representativesOfNewQuery" -> representativesOfNewQuery,
    "representativesOfNaiveQuery" -> representativesOfNaiveQuery,
    "naiveExecutorRepresentativeness" -> naiveExecutorRepresentativeness,
    "suppWrtNaive" -> suppWrtNaive
  ).toSeq.sortBy(_._1)
}