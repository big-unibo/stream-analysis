package it.unibo.big.query.app.common

import it.unibo.big.input.{AlgorithmConfiguration, ConfigurationSetting, NaiveConfiguration, SimulationConfiguration, StreamAnalysisConfiguration}
import it.unibo.big.input.DataDefinition.{NullData, NumericData, StringData}
import it.unibo.big.input.GPSJConcepts.GPQuery
import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.query.algorithm.QueryUpdate
import it.unibo.big.query.algorithm.naive.NaiveQueriesExecutor
import it.unibo.big.query.app.DatasetsUtils.Dataset
import it.unibo.big.query.debug.DebugWriter
import it.unibo.big.query.debug.DebugWriter.LastPaneStatistics
import it.unibo.big.query.generation.countdistinct.CountDistinct.getDataAsNullValue
import it.unibo.big.query.generation.countdistinct.ShlosserEstimator
import it.unibo.big.query.state.StateUtils.StateType
import it.unibo.big.query.state.{QueryStatisticsInPane, State, StateUtils}
import org.slf4j.{Logger, LoggerFactory}
import it.unibo.big.window.DataWindow.windowing

object WindowedQueryExecution {

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * Get the support of the dimensions
   * @param data the data
   * @return the support of the dimensions
   */
  private def getDimensionsSupport(data: Seq[Record]): Map[String, Double] = {
    data.flatMap(d => d.dimensions.map(dim => {
      dim -> (if (getDataAsNullValue(d, dim) != NullData) 1 else 0)
    }).toSeq).groupBy(_._1).map {
      case (dim, seq) => dim -> seq.map(_._2).sum.toDouble / data.size
    }
  }

    /**
   * Compute the queries with given configuration
   *
   * @param data the data to consider
   * @param configurations the map of naive configurations and the corresponding algorithm configurations
   * @param simulationConfiguration the simulation configuration
   * @param setState the function to set the state of the algorithm starting from the naive configuration and the algorithm configurations and the current configuration (default is that each algorithm have its own state)
   */
  def compute(data: Iterator[Record], configurations: Map[ConfigurationSetting, (Option[NaiveConfiguration], Seq[StreamAnalysisConfiguration])],
              simulationConfiguration: SimulationConfiguration,
              setState: (Option[State], Map[StreamAnalysisConfiguration, State], AlgorithmConfiguration) => State = StateUtils.OwnState.setState): Unit = {
    //require that all the configurations have the same window duration and slide duration and pattern and inputSimulationFile
    require(configurations.nonEmpty)
    configurations.foreach{
      case (sett, (n, algorithmConfigurations)) =>
        if(n.isDefined) {
          sett.isCompliantConfiguration(n.get)
        }
        algorithmConfigurations.foreach(a => sett.isCompliantConfiguration(a))
    }
    val states = configurations.map{
      case (sett, (n, configurations)) => (sett, (if(n.isDefined) Some(State(Map(), n.get)) else None, configurations.map(c => c -> State(Map(), c)).toMap))
    }

    LOGGER.info(s"Window duration = ${simulationConfiguration.windowDuration} slide duration = ${simulationConfiguration.slideDuration}")
    windowing[Record](data, simulationConfiguration.windowDuration, simulationConfiguration.slideDuration, (window, data, _) => {
      val newPaneData = data._2
      val windowData = data._1
      val previousPaneData = data._3

      //info application statistics
      val dataDimensions = getDimensionsSupport(newPaneData)
      val dimensions = dataDimensions.keySet
      val measures = newPaneData.flatMap(_.measures).toSet.diff(dimensions)
      dataDimensions.foreach{
        case (d, s) =>
          LOGGER.info(s"Dimension $d with support $s")
      }

      states.foreach {
        case (_, (gfsNaive, gfsMap)) =>
          var naiveSelectedQuery : Option[(GPQuery, QueryStatisticsInPane)] = None

          val naiveConfiguration = gfsNaive.map(_.configuration)
          if(naiveConfiguration.nonEmpty) {
            try {
              naiveSelectedQuery = NaiveQueriesExecutor.apply(simulationConfiguration, setState(gfsNaive, gfsMap, naiveConfiguration.get), newPaneData, window, dimensions, measures)
            } catch {
              case e: Exception =>
                LOGGER.error(s"Error in naive algorithm", e)
            }
          }
          var lastPaneStatistics : LastPaneStatistics = null
          if(naiveConfiguration.isDefined) {
            val (_, allDimensionsStatistics, functionalDependenciesScores) = ShlosserEstimator.calculateFunctionalDependencies(newPaneData, naiveConfiguration.get, naiveSelectedQuery.map(_._1), dimensions, calculateOnlyForInputQuery = true)
            /*//remove old data from the map
            countDMap = countDMap.filterKeys(window.contains)
            //filter out the functional dependencies of previous windows
            countDMap = countDMap.map{
              case (w, (dataSize, dimensionStatistics, _)) => (w, (dataSize, dimensionStatistics, Map[(String, String), Double]()))
            }*/
            lastPaneStatistics = (newPaneData.size, allDimensionsStatistics, functionalDependenciesScores)
            LOGGER.info(s"Window $window (t = ${window.paneTime}) with ${windowData.size} values new data = ${newPaneData.size} old data = ${previousPaneData.size}")
          }

          gfsMap.foreach {
            case (c, gfs) =>
              try {
                val (newQuery, executedQueries) = QueryUpdate.updateQuery(simulationConfiguration, setState(gfsNaive, gfsMap, c), newPaneData, window, dimensions, measures, writeDebug = true)
                if (newQuery.isDefined) {
                  val (result, queryPanesSupport) = gfs.compute(window, newQuery.get)
                  LOGGER.info(s"[QUERY ${newQuery.get.dimensions}] $queryPanesSupport % panes with query config = $c")
                  LOGGER.info(s"Query ${newQuery.get}")
                  val keys = newQuery.get.dimensions.toList ++ newQuery.get.aggregations.map(_.toString).toList
                  LOGGER.debug(keys.mkString("|"))
                  //var resultRecords = Seq[Seq[Any]]()
                  result.foreach(x => {
                    val r = keys.map(k => x.getOrElse(k, NullData)).map {
                      case NumericData(value) => "%.2f".format(value.doubleValue())
                      case StringData(value) => value
                      case NullData => null
                    }
                    /*resultRecords = resultRecords :+ keys.map(k => x.getOrElse(k, NullData)).map {
                    case NumericData(value) => value
                    case StringData(value) => value
                    case NullData => null
                  }*/
                    LOGGER.debug(r.mkString("|")) //TODO write as output statistics
                  })
                  //if(c.knapsack.isDefined) {
                  //write a csv file with the result of the query of the algorithm in the window
                  // FileWriter.writeFileWithHeader(resultRecords, keys, s"debug/analysis/results/results_${c.name}_${window.paneTime}.csv")
                  //}
                  DebugWriter.writeDatasetStatistics(window, dataDimensions, if (naiveSelectedQuery.isDefined) Some(lastPaneStatistics._2) else None, simulationConfiguration)
                }
              } catch {
                case e: Exception =>
                  LOGGER.error(s"Error in computing query with configuration $c", e)
              }
          }
      }
    }, None, None, numberOfWindowsToConsider = simulationConfiguration.numberOfWindowsToConsider)
    LOGGER.info(s"End simulation")
  }

    /**
   * Simulate the given query pattern
   * @param dataset the dataset
   * @param windowDuration the window duration
   * @param slideDuration the slide duration
   * @param numberOfWindowsToConsider the number of windows to consider
   * @param stateType the state type
   * @param configurations the configurations settings with optional naive algorithm and the corresponding algorithm configurations
   * @param availableTime the available time for the simulation
   */
  def simulate(dataset: Dataset, windowDuration: Long, slideDuration: Long, numberOfWindowsToConsider: Int,
               stateType: StateType, configurations: Map[ConfigurationSetting, (Option[NaiveConfiguration], Seq[StreamAnalysisConfiguration])], availableTime: Long): Unit = {
    val path = dataset.path(stateType)
    /*LOGGER.info(s"Start simulation in $path")
    FileWriter.deleteDirectory(path)
    LOGGER.info(s"Deleting directory $path")*/

    val simulationConfiguration = SimulationConfiguration(dataset,
      windowDuration = windowDuration,
      slideDuration = slideDuration,
      numberOfWindowsToConsider = Some(numberOfWindowsToConsider),
      statisticsFile = s"${path}stats.csv",
      datasetStatisticsFile = s"${path}stats_dataset.csv",
      availableTime = availableTime
    )
    /*
    windowing[Record](data, simulationConfiguration.windowDuration, simulationConfiguration.slideDuration, (window, data, windowTime) => {
       LOGGER.info(s"Window time $windowTime for dataset $datasetName")
     }, None, None, numberOfWindowsToConsider = Some(simulationConfiguration.numberOfWindowsToConsider))*/
    try {
      compute(dataset.reader, configurations, simulationConfiguration, stateType.setState)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        LOGGER.error(s"Error in computing $dataset", e)
    }
  }
}
