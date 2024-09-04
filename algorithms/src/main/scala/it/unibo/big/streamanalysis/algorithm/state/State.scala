package it.unibo.big.streamanalysis.algorithm.state

import it.unibo.big.streamanalysis.input.DataDefinition.Data
import it.unibo.big.streamanalysis.input.GPSJConcepts.GPQuery
import it.unibo.big.streamanalysis.input.RecordModeling.Window
import it.unibo.big.streamanalysis.algorithm.generation.QueryUtils.QueriesWithStatistics
import StateDefinitionUtils.{InternalState, aggregateQueryResults}
import it.unibo.big.streamanalysis.input.AlgorithmConfiguration
import it.unibo.big.streamanalysis.input.DataDefinition.Data
import it.unibo.big.streamanalysis.input.GPSJConcepts.GPQuery
import it.unibo.big.streamanalysis.input.RecordModeling.Window
import org.slf4j.{Logger, LoggerFactory}

/**
 * Class for algorithm in a sliding window paradigm.
 *
 * @param internalStatus the internal state of the algorithm
 * @param configuration  the configuration used
 * */
case class State(private var internalStatus: InternalState, configuration: AlgorithmConfiguration) {

  private val timeStatistics = AlgorithmExecutionTimeStatistics()
  private var functionalDependencyScores: Map[Long,  Map[(String, String), Double]] = Map()
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   *
   * @return the time statistics
   */
  def getTimeStatistics: AlgorithmExecutionTimeStatistics = timeStatistics

  /**
   * Update the data structure removing the old panes
   *
   * @param window the window used
   * @return the previous selected query
   */
  def update(window: Window): Option[GPQuery] = {
    val time = System.currentTimeMillis()
    functionalDependencyScores = functionalDependencyScores.filterKeys(x => window.contains(x))
    internalStatus = internalStatus.filterKeys(x => window.contains(x) && x != window.paneTime)
    //reset the status at pane time, if it is from another algorithm we will make it empty
    val previousSelectedQuery = if(internalStatus.nonEmpty) getSelectedQueryAtTimestamp(internalStatus.maxBy(_._1)._1) else None
    internalStatus += (window.paneTime -> Map())
    timeStatistics.addTimeForUpdateWindow(System.currentTimeMillis() - time)
    previousSelectedQuery
  }

  /**
   * Compute the query
   *
   * @param window the window used
   * @param query  the query to compute
   * @return the result of the query and the percentage of panes that have the query results
   */
  def compute(window: Window, query: GPQuery): (Seq[Map[String, Data[_]]], Double) = {
    val time = System.currentTimeMillis()
    var timeForAggregation = 0L
    val results = internalStatus.collect {
      case (t, paneStatistics) if paneStatistics.contains(query) && window.contains(t) && paneStatistics(query).isStored => paneStatistics(query).getQueryResult.get
    }.flatten.groupBy(_._1).map {
      case (d, statistics) =>
        val t = System.currentTimeMillis()
        val res = aggregateQueryResults(query, d, statistics.map(_._2).toSeq)
        timeForAggregation += System.currentTimeMillis() - t
        res
    }.toSeq

    val queryPanesSupport = internalStatus.count(_._2.contains(query)).toDouble / internalStatus.size * 100
    //shape the final query result
    val returnValue = (results.map(queryResults => queryResults.dimensions ++ queryResults.measures.map { case (agg, value) => agg.toString -> value }), queryPanesSupport)
    LOGGER.info(s"Query ${query.dimensions} computed in ${System.currentTimeMillis() - time} ms (aggregation = $timeForAggregation ms)")
    timeStatistics.addTimeForComputeQueryInTheWindow(System.currentTimeMillis() - time)
    returnValue
  }

  /**
   *
   * @param paneTime the time of the pane
   * @return the query selected at timestamp if present
   */
  private def getSelectedQueryAtTimestamp(paneTime: Long): Option[GPQuery] = {
    util.Try(internalStatus(paneTime).find(_._2.isSelectedQuery).map(_._1).get).toOption
  }

  /**
   * Add the queries that can be executed
   *
   * @param queriesWithStatistics the queries with statistics for the given pane time
   * @param fds the scores of the functional dependency
   * @param paneTime              the time of the pane
   */
  def addQueriesThatCanBeExecuted(queriesWithStatistics: QueriesWithStatistics, fds: Map[(String, String), Double], paneTime: Long): Unit = {
    internalStatus += paneTime -> queriesWithStatistics
    functionalDependencyScores += paneTime -> fds
  }

  /**
   * Get the queries in the pane
   * @param paneTime the time of the pane
   * @return the queries with their statistics for the given pane time
   */
  def getQueriesInPane(paneTime: Long): Map[GPQuery, QueryStatisticsInPane] = internalStatus(paneTime)

  /**
   * Get the scores of the query
   *
   * @param query the query
   * @param lastConsideredPane the considered pane, if not present the real support is returned
   * @return the supports of the query over the time, real for all the timestamp and estimated for the last considered time
   *
   */
  def getSupports(query: GPQuery, lastConsideredPane: Option[Long]): Map[Long, Double] = {
    val time = System.currentTimeMillis()
    val returnValue = internalStatus.toSeq.map {
      case (t, paneStatistics) if lastConsideredPane.contains(t) && paneStatistics.contains(query) => t -> paneStatistics(query).score.paneSupport
      case (t, paneStatistics) if paneStatistics.contains(query) => t -> paneStatistics(query).realSupport
      case (t, _) => t -> 0D
    }.toMap
    timeStatistics.addTimeForGettingScores(System.currentTimeMillis() - time)
    returnValue
  }

  /**
   * Get the score of the query
   *
   * @param query the query
   * @return the query statistics average and the statistics for each pane
   */
  def getQueryStatistics(query: GPQuery): Map[Long, Option[QueryStatisticsInPane]] = internalStatus.map(x => x._1 -> x._2.get(query))

  /**
   * Update the data structure
   *
   * @param paneTime      the pane time
   * @param query         the query
   * @param queryResult   the query result of the given query in the given pane
   * @param executionTime the execution time of the query
   * @param inputRecords  the number of records input
   */
  def update(paneTime: Long, executionTime: Long, query: GPQuery, queryResult: Map[Map[String, Data[_]], QueryResultSimplified], inputRecords: Int): Unit = {
    val time = System.currentTimeMillis()
    timeStatistics.addTimeForQueryExecution(executionTime)
    internalStatus(paneTime)(query).addQueryResult(queryResult, executionTime, inputRecords)
    timeStatistics.addTimeForUpdateThePane(System.currentTimeMillis() - time)
  }

  /**
   * Update the data structure without storing the results
   *
   * @param paneTime      the pane time
   * @param executionTime the execution time
   * @param query         the query
   * @param resultSize    the size of the result
   * @param inputRecords  the number of records input
   */
  def updateExecutionWithoutStoreResults(paneTime: Long, executionTime: Long, query: GPQuery, resultSize: Int, inputRecords: Int): Unit = {
    val time = System.currentTimeMillis()
    timeStatistics.addTimeForQueryExecution(executionTime)
    internalStatus(paneTime)(query).addQueryExecutionWithoutResult(resultSize, executionTime, inputRecords)
    timeStatistics.addTimeForUpdateThePane(System.currentTimeMillis() - time)
  }

  /**
   * @return the actual of panes
   */
  def numberOfPanes: Int = internalStatus.size

  /**
   * Check if the query is present in the panes
   *
   * @param q the query
   * @return true if the query is present in the panes
   */
  def contains(q: GPQuery): Boolean = internalStatus.exists(_._2.contains(q))

  /**
   * Change the internal state
   *
   * @param other the other state
   * @return the state with the new state
   */
  def setState(other: State): State = {
    this.internalStatus = other.internalStatus
    this
  }

  /**
   * Get the queries
   *
   * @param paneTime the time of the pane
   * @return the queries with their statistics for the given pane time
   */
  def getQueries(paneTime: Long): Map[GPQuery, QueryStatisticsInPane] = internalStatus(paneTime)

  /**
   *
   * @return the functional dependency scores for each timestamp
   */
  def getFDSForEachTimeStamp: Map[Long, Map[(String, String), Double]] = functionalDependencyScores

  /**
   * Get the queries cardinality
   * @return the cardinalities of the queries in all the panes
   */
  def getQueryCardinalities: Map[Long, Map[GPQuery, Long]] = {
    internalStatus.mapValues(_.map{
        case (q, statistics) => q -> statistics.realNumberOfRecords.getOrElse(statistics.estimatedNumberOfRecords)
      })
  }
}
