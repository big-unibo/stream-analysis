package it.unibo.big.query.debug

import it.unibo.big.input.GPSJConcepts.GPQuery
import it.unibo.big.input.RecordModeling.{Record, Window}
import it.unibo.big.input.SimulationConfiguration
import it.unibo.big.query.generation.QueryUtils.QueriesWithStatistics
import it.unibo.big.query.generation.countdistinct.DimensionStatistics.DimensionStatistic
import it.unibo.big.query.state.{QueryStatisticsInPane, State}
import it.unibo.big.utils.FileWriter
import org.slf4j.{Logger, LoggerFactory}

object DebugWriter {

  type LastPaneStatistics = (Int, Map[String, DimensionStatistic], Map[(String, String), Double])

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * Write the statistics of the queries
   * @param simulationConfiguration the simulation configuration
   * @param state the algorithm state
   * @param selectedQuery the selected query with score
   * @param window the window
   * @param totalTime the total time of the simulation
   * @param previousChosenQuery the previous chosen query
   * @param data the data
   * @param numberOfQueriesToExecute the number of queries to execute
   */
  def writeStatistics(simulationConfiguration: SimulationConfiguration, state: State, selectedQuery: Option[GPQuery], window: Window, totalTime: Long, previousChosenQuery: Option[GPQuery], data: Seq[Record], numberOfQueriesToExecute: Int): Unit = {
    LOGGER.debug(s"Writing statistics for ${state.configuration}")
    val numberOfAttributes = data.flatMap(_.data.keySet).distinct.size
    val queriesWithTotalScore: QueriesWithStatistics = state.getQueries(window.paneTime)
    //write file statistics
    val statistics: Seq[DebugQueryStatistics] = queriesWithTotalScore.map {
      case (q, _) =>
        DebugQueryStatistics(
          window = window,
          simulationConfiguration = simulationConfiguration,
          query = q,
          previousChosenQuery = previousChosenQuery,
          selectedQuery = selectedQuery,
          state = state,
          totalTime = totalTime,
          numberOfAttributes = numberOfAttributes,
          numberOfQueriesToExecute = numberOfQueriesToExecute
        )
    }.toSeq

    FileWriter.writeFileWithHeader(statistics.map(_.toSeq), statistics.headOption.map(_.header).getOrElse(Seq()), simulationConfiguration.statisticsFile)
  }

  /**
   * Write the dataset statistics
   * @param window the window
   * @param dataDimensions the data dimensions
   * @param dataStatistics the data statistics, if present
   * @param simulationConfiguration the simulation configuration
   */
  def writeDatasetStatistics(window: Window, dataDimensions: Map[String, Double], dataStatistics: Option[Map[String, DimensionStatistic]], simulationConfiguration: SimulationConfiguration): Unit = {
    val header = Seq("paneTime", "windowStart", "windowEnd", "dimension", "support", "count distinct")
    val statistics = dataDimensions.map{
      case (d, v)  =>
        val stat = util.Try(dataStatistics.get(d)).toOption
        Seq(window.paneTime, window.start.getTime, window.end.getTime, d, stat.map(_.support).getOrElse(v), stat.map(_.countD.asInstanceOf[Any]).orNull)
    }.toSeq

    FileWriter.writeFileWithHeader(statistics, header, simulationConfiguration.datasetStatisticsFile)
  }
}
