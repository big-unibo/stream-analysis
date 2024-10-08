package it.unibo.big.streamanalysis.algorithm.generation.choosing

import it.unibo.big.streamanalysis.algorithm.state.QueryStatisticsInPane
import it.unibo.big.streamanalysis.input.GPSJConcepts.GPQuery
import it.unibo.big.streamanalysis.input.StreamAnalysisConfiguration
import optimus.algebra.AlgebraOps._
import optimus.algebra._
import optimus.optimization._
import optimus.optimization.enums.SolverLib
import optimus.optimization.model.MPBinaryVar
import org.slf4j.{Logger, LoggerFactory}

object Knapsack {

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  private case class Item(estimatedRecords: Int, score: Double, x: MPBinaryVar, query: GPQuery)
  /**
   *
   * @param items the queries to select, with their statistics
   * @param maximumNumberOfRecords the maximum number of estimatedRecords
   * @param configuration the used configuration
   * @param numberOfQueriesToExecute the number of queries to execute
   * @param timeLimit the time limit in milliseconds for run the knapsack
   * @return the selected queries that maximize the scores, considering the constraints
   */
  def apply(items: Seq[(GPQuery, QueryStatisticsInPane)], maximumNumberOfRecords: Long, configuration: StreamAnalysisConfiguration, numberOfQueriesToExecute: Int, timeLimit: Long): Seq[GPQuery] = {
    try {
      implicit val knapsackProblem: MPModel = MPModel(SolverLib.oJSolver)

      val itemsKnapsack = Array.tabulate(items.size)(i => Item(items(i)._2.estimatedNumberOfRecords.toInt, items(i)._2.score.value, MPBinaryVar(s"x$i"), items(i)._1))

      maximize(sum(itemsKnapsack)(item => item.x * item.score))

      // Given the limited capacity of the pack
      subjectTo {
        Seq(sum(itemsKnapsack)(item => item.x * item.estimatedRecords) <:= maximumNumberOfRecords,
          sum(itemsKnapsack)(item => item.x) <:= numberOfQueriesToExecute): _*
      }

      start(timeLimit = timeLimit.toInt)

      // Extract the solution
      val selected = itemsKnapsack.filter(item => item.x.value.get.toInt == 1)
      val selectedItems: Seq[GPQuery] = selected.map(_.query)
      val totalScore: Double = selected.map(_.score).sum

      // Calculate total space used
      val totalSpaceUsed: Long = selected.map(item => item.estimatedRecords).sum
      if (totalSpaceUsed > maximumNumberOfRecords) {
        LOGGER.error(s"Total space used $totalSpaceUsed is greater than maximumNumberOfRecords $maximumNumberOfRecords")
      }
      if (totalSpaceUsed == 0) {
        LOGGER.warn("Space used is 0")
      }
      if (selectedItems.size > numberOfQueriesToExecute) {
        LOGGER.error("Executing more queries than expected")
      }
      if (selectedItems.isEmpty) {
        LOGGER.warn("No queries selected")
      }
      if (selectedItems.size != selectedItems.toSet.size) {
        LOGGER.error("Duplicate queries selected")
      }
      LOGGER.info(s"Total space used: $totalSpaceUsed/$maximumNumberOfRecords, total score: $totalScore, Selected items ${selectedItems.size}/$numberOfQueriesToExecute")
      LOGGER.debug(s": ${selectedItems.map(_.dimensions.mkString(",")).mkString("\n")}")

      release()
      selectedItems
    } catch {
      case e: Exception =>
        LOGGER.error("Error in knapsack", e)
        Seq.empty
    }
  }
}
