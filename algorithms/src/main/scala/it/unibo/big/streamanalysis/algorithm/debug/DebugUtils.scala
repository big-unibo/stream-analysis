package it.unibo.big.streamanalysis.algorithm.debug

import it.unibo.big.streamanalysis.algorithm.state.State
import it.unibo.big.streamanalysis.input.GPSJConcepts.GPQuery
import it.unibo.big.streamanalysis.input.RecordModeling.Window
import it.unibo.big.streamanalysis.input.{AlgorithmConfiguration, NaiveConfiguration, SimulationConfiguration, StreamAnalysisConfiguration}

/**
 * DebugUtils is the object that contains the utility functions for the debug
 */
object DebugUtils {

  /**
   * DebugStatistics is the trait for the debug statistics into files
   */
  trait DebugStatistics {
    /**
     * @return The window of the statistic
     */
    def window: Window

    /**
     *
     * @return The query to output statistics
     */
    def query: GPQuery

    /**
     *
     * @return The simulation configuration
     */
    def simulationConfiguration: SimulationConfiguration

    /**
     *
     * @return The number of query to execute
     */
    def numberOfQueriesToExecute: Int

    private def single: Boolean = algorithmConfiguration match {
      case x: StreamAnalysisConfiguration => x.singleQuery
      case _ => false
    }

    /**
     *
     * @return The state of the algorithm with the configuration
     */
    def state: State

    def data: Seq[(String, Any)] = Map(
      "paneTime" -> window.paneTime,
      "windowStart" -> window.start.getTime,
      "windowEnd" -> window.end.getTime,
      "windowDuration" -> simulationConfiguration.windowDuration,
      "slideDuration" -> simulationConfiguration.slideDuration,
      "inputFile" -> simulationConfiguration.dataset.fileName,
      "alpha" -> algorithmConfiguration.alpha,
      "beta" -> algorithmConfiguration.beta,
      "pattern.numberOfDimensions" -> algorithmConfiguration.pattern.numberOfDimensions,
      "percentageOfRecordsInQueryResults" -> algorithmConfiguration.percentageOfRecordsInQueryResults,
      "logFactor" -> getValue(state, _.logFactor).getOrElse(Int.MaxValue),
      "knapsack" -> getValue(state, _.knapsack.nonEmpty).getOrElse(false),
      "numberOfQueriesToExecute" -> numberOfQueriesToExecute,
      "dimensions" -> formattedString(query.dimensions.toSeq.sorted),
      "single" -> single
    ).toSeq

    /**
     *
     * @return The statistics as a sequence of Any to write in the output for the debug
     */
    def toSeq: Seq[Any] = data.map(_._2)

    /**
     *
     * @return The header of the statistics
     */
    def header: Seq[String] = data.map(_._1)

    /**
     *
     * @return The configuration of the algorithm
     */
    def algorithmConfiguration: AlgorithmConfiguration  = state.configuration
  }

  /**
   *
   * @param state the algorithm state
   * @param fun a function that starts from a StreamAnalysisConfiguration and return a value
   * @tparam T the type of the returned value
   * @return if the state have a configuration that is instance of StreamAnalysisConfiguration, the function fun is applied
   *         and is returned an option with inside the returned value, otherwise None
   */
  private [debug] def getValue[T](state: State, fun: StreamAnalysisConfiguration => T): Option[T] = {
    state.configuration match {
      case x: StreamAnalysisConfiguration => Some(fun(x))
      case _: NaiveConfiguration => None
    }
  }

  /**
   * Format the sequence of values
   * @param seq the sequence of values
   * @return the formatted string
   */
  private [debug] def formattedString(seq: Seq[Any]): String = seq.mkString(",")
}
