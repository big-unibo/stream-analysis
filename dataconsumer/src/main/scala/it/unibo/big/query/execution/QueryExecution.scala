package it.unibo.big.query.execution

import it.unibo.big.input.DataDefinition._
import it.unibo.big.input.GPSJConcepts.GPQuery
import it.unibo.big.input.RecordModeling.{Record, Window}
import it.unibo.big.input.{SimulationConfiguration, StreamAnalysisConfiguration}
import it.unibo.big.query.debug.DebugWriter
import it.unibo.big.query.generation.choosing.Knapsack
import it.unibo.big.query.generation.choosing.ScoreUtils._
import it.unibo.big.query.state.{QueryResultSimplified, QueryStatisticsInPane, State}
import org.slf4j.{Logger, LoggerFactory}

object QueryExecution {

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  private val AVAILABLE_TIME_EXTRA = 10 //Extra time to consider for the queries

  /**
   * Execute the queries
   * @param simulationConfiguration the simulation configuration
   * @param algorithmState the algorithm state
   * @param window the window
   * @param data the data
   * @param inputQuery the input query if present
   * @param startTime the start time of the simulation
   * @param queriesStatisticsTime time used for compute queries statistics
   * @param writeDebug if true write debug information
   * @return the new selected query and the number of queries to execute
   */
  def executeQueries(simulationConfiguration: SimulationConfiguration, algorithmState: State, window: Window, data: Seq[Record], inputQuery: Option[GPQuery], startTime: Long,
                     queriesStatisticsTime: Long, writeDebug: Boolean): (Option[GPQuery], Int) = {
    val configuration = algorithmState.configuration.asInstanceOf[StreamAnalysisConfiguration]

    val time = System.currentTimeMillis()
    val maxNumberOfRecords = configuration.getMaximumOfRecordsToStore(data)
    val queries = algorithmState.getQueries(window.paneTime).filter(_._2.exists) // and filter out queries that not exists
    //sort query for deterministic behavior
    val sortedQueries = sortQueries(queries)
    algorithmState.getTimeStatistics.addTimeForChooseQueries(System.currentTimeMillis() - time)
    val usedTime = queriesStatisticsTime + AVAILABLE_TIME_EXTRA
    val availableTime = simulationConfiguration.availableTime - usedTime
    val timeForQueryComputation = configuration.timeForQueryComputation(simulationConfiguration, configuration.pattern.numberOfDimensions, availableTime)
    //subtract a query computation time for knapsack execution
    val numberOfQueriesToExecuteTmp = math.max(math.floor((availableTime - (if(configuration.knapsack.nonEmpty) timeForQueryComputation else 0L)) / timeForQueryComputation).toInt, 0)
    val numberOfQueriesToExecute = if(configuration.singleQuery) math.min(numberOfQueriesToExecuteTmp, 1) else numberOfQueriesToExecuteTmp
    if(numberOfQueriesToExecute <= 0) {
      LOGGER.error(s"Number of queries to execute must be greater than 0, " +
        s"but it is $numberOfQueriesToExecute for available time $availableTime/${simulationConfiguration.availableTime} " +
        s"input file = ${simulationConfiguration.dataset.fileName} and dimensions = ${configuration.pattern.numberOfDimensions}")
      if(writeDebug) {
        DebugWriter.writeStatistics(simulationConfiguration, algorithmState, inputQuery, window, System.currentTimeMillis() - startTime, inputQuery, data, 0)
      }
      return (inputQuery, 0)
    }

    val queriesToExecute = if(configuration.knapsack.nonEmpty) {
      LOGGER.info(s"Using knapsack to choose queries $configuration")
      val startKnapsackTime = System.currentTimeMillis()
      val queriesToExecute = Knapsack.apply(sortedQueries, maxNumberOfRecords, configuration, numberOfQueriesToExecute, timeForQueryComputation)
      LOGGER.info(s"Queries selected by knapsack: ${queriesToExecute.size}/$numberOfQueriesToExecute")
      //sort the queries considering the knapsack selection
      val queriesToExecuteSorted = sortQueries(queries, q => if(queriesToExecute.contains(q)) 0 else 1)
      algorithmState.getTimeStatistics.addTimeForChooseQueries(System.currentTimeMillis() - startKnapsackTime)
      queriesToExecuteSorted
    } else {
      LOGGER.info(s"Using rank approach to choose queries $configuration")
      sortedQueries
    }

    val (selectedQuery, executedQueries) = executeQueries(algorithmState, window, data, simulationConfiguration, configuration, maxNumberOfRecords, inputQuery, queriesToExecute, numberOfQueriesToExecute,
      usedTime + (System.currentTimeMillis() - time))

    val totalTime = System.currentTimeMillis() - startTime
    //Take maximum number of queries between estimated and executed
    val realNumberOfQueries = math.max(executedQueries, numberOfQueriesToExecute)

    if(writeDebug) {
      DebugWriter.writeStatistics(simulationConfiguration, algorithmState, selectedQuery.map(_._1), window, totalTime, inputQuery, data, realNumberOfQueries)
    }
    //reset the time statistics
    algorithmState.getTimeStatistics.reset()

    if(selectedQuery.nonEmpty) {
      if(inputQuery.isEmpty) {
        LOGGER.info(s"[START QUERY] ${selectedQuery.get}")
      } else {
        if(inputQuery.get.dimensions != selectedQuery.get._1.dimensions) {
          LOGGER.info(s"[CHANGE QUERY] ${selectedQuery.get._1.dimensions} SUBSTITUTES ${inputQuery.get.dimensions}")
        }
      }
    }
    (selectedQuery.map(_._1), realNumberOfQueries)
  }

  /**
   * Execute the queries
   *
   * @param algorithmState the algorithm state
   * @param window           the window
   * @param data               the dataset
   * @param simulationConfiguration the simulation configuration
   * @param configuration      the configuration
   * @param maximumStoredRecords       the maximum number of records that can be stored for the pane
   * @param inputQuery        the input query (if present)
   * @param queriesToExecute   the queries to execute, with their statistics. Already sorted.
   * @param numberOfQueriesToExecute the number of queries to execute
   * @return the new selected query with the total SOP (if present) and the number of executed queries
   */
  private def executeQueries(algorithmState: State,
                             window: Window, data: Seq[Record],
                             simulationConfiguration: SimulationConfiguration,
                             configuration: StreamAnalysisConfiguration,
                             maximumStoredRecords: Int,
                             inputQuery: Option[GPQuery],
                             queriesToExecute: Seq[(GPQuery, QueryStatisticsInPane)], numberOfQueriesToExecute: Int, usedTime: Long): (Option[(GPQuery, QueryStatisticsInPane)], Int) = {
    var queryIndex = 0
    var storedRecords = 0L
    var executedQueries = 0
    var firstExecutedQuery : Option[(GPQuery, QueryStatisticsInPane)] = None
    val paneTime = window.paneTime
    var actualUsedTime = usedTime

    LOGGER.info(s"Queries aim to execute: ${queriesToExecute.size}/$numberOfQueriesToExecute, with a maximum of $maximumStoredRecords estimatedRecords")
    while (storedRecords < maximumStoredRecords
      && canExecuteQueryConsideringTime(simulationConfiguration, configuration, actualUsedTime, executedQueries)
      && executedQueries < queriesToExecute.size && queryIndex < queriesToExecute.size) {
      val t = System.currentTimeMillis()
      var queryExecutionTime = 0L
      val (query, qs) = queriesToExecute(queryIndex)
      if (storedRecords + qs.estimatedNumberOfRecords <= maximumStoredRecords) {
        LOGGER.debug(s"[EXECUTING] Executing query ${query.dimensions} with score ${qs.score.value} and ${qs.estimatedNumberOfRecords} estimated number of records")
        // execute query
        val startQueryExecutionTime = System.currentTimeMillis()
        executeQuery(algorithmState, data, query, paneTime, storedRecords, maximumStoredRecords)
        executedQueries += 1
        if (qs.isStored) {
          storedRecords += qs.realNumberOfRecords.get //Update the used estimatedRecords with the real number of records
          if(firstExecutedQuery.isEmpty) {
            firstExecutedQuery = Some(query, qs)
          } else {
            if(firstExecutedQuery.get._2.score.value < qs.score.value) {
              firstExecutedQuery = Some(query, qs)
            }
          }
        }
        queryExecutionTime = System.currentTimeMillis() - startQueryExecutionTime
      } else {
        LOGGER.debug(s"Skipping query ${query.dimensions} with score  ${qs.score.value} and ${qs.estimatedNumberOfRecords} estimatedRecords (we have already $storedRecords/$maximumStoredRecords estimatedRecords)")
      }
      queryIndex += 1
      algorithmState.getTimeStatistics.addTimeForChooseQueries(System.currentTimeMillis() - t - queryExecutionTime)
      actualUsedTime += System.currentTimeMillis() - t
    }
    LOGGER.info(s"Queries executed: $executedQueries/$numberOfQueriesToExecute, with $storedRecords/$maximumStoredRecords estimatedRecords")

    val time = System.currentTimeMillis()
    val selectedQuery = getBestQuery(paneTime, algorithmState, inputQuery, firstExecutedQuery)
    algorithmState.getTimeStatistics.addTimeForChooseQueries(System.currentTimeMillis() - time)
    (selectedQuery, executedQueries)
  }

  /**
   *
   * @param simulationConfiguration the simulation configuration
   * @param configuration the configuration
   * @param actualUsedTime the actual used time
   * @return true if the query can be executed considering the time
   */
  private def canExecuteQueryConsideringTime(simulationConfiguration: SimulationConfiguration, configuration: StreamAnalysisConfiguration, actualUsedTime: Long, numberOfExecutedQueries: Int): Boolean = {
    val canExecuteTmp = (actualUsedTime + configuration.timeForQueryComputation(simulationConfiguration, configuration.pattern.numberOfDimensions, simulationConfiguration.availableTime - actualUsedTime)) < simulationConfiguration.availableTime
    if(configuration.singleQuery && numberOfExecutedQueries >= 1) false else canExecuteTmp
  }

  /**
   * Execute the query updating its statistics
   * @param algorithmState the algorithm state
   * @param data the data
   * @param query the query
   * @param paneTime the time of the pane
   * @param numberOfRecords the number of records stored before the query execution
   * @param maxNumberOfRecords the maximum number of records to have in the queryResult
   */
  def executeQuery(algorithmState: State, data: Seq[Record], query: GPQuery, paneTime: Long, numberOfRecords: Long, maxNumberOfRecords: Long): Unit = {
    val startExecuteQueryTime = System.currentTimeMillis()
    val queryResult: Map[Map[String, Data[_]], QueryResultSimplified] = data
      .groupBy(x => query.dimensions.map(d => d -> x.data.getOrElse[Data[_]](d, NullData)).toMap[String, Data[_]]).map {
        case (k, v) => k -> QueryResultSimplified(paneTime, query, k, v)
      }
    if(numberOfRecords + queryResult.size > maxNumberOfRecords) {
      LOGGER.warn(s"Query result goes over the maximum number of estimatedRecords, returning empty map - result size = ${queryResult.size} + $numberOfRecords")
      algorithmState.updateExecutionWithoutStoreResults(paneTime, executionTime = System.currentTimeMillis() - startExecuteQueryTime, query = query, resultSize = queryResult.size, inputRecords = data.size)
    } else {
      algorithmState.update(paneTime, executionTime = System.currentTimeMillis() - startExecuteQueryTime, query = query, queryResult = queryResult, inputRecords = data.size)
    }
  }
}
