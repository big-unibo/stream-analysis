package it.unibo.big.query.state

import it.unibo.big.input.DataDefinition.{Data, NullData}
import it.unibo.big.input.GPSJConcepts.GPQuery
import it.unibo.big.input.NaiveConfiguration
import it.unibo.big.query.generation.choosing.ScoreUtils.Score

/**
 * Case class for the query statistics in a pane
 *
 * @param query                    the query
 * @param estimatedNumberOfRecords the estimated number of records
 * @param scoreValue               the total score of the query
 * @param estimatedTime            the estimated time for the query
 * @param exists                   if the query exists, false only if a previous chosen query not exists anymore or can not be executed
 */
case class QueryStatisticsInPane(query: GPQuery, estimatedNumberOfRecords: Long, private var scoreValue: Score, estimatedTime: Long, exists: Boolean = true) {
  private var result: Option[Map[Map[String, Data[_]], QueryResultSimplified]] = None
  private var queryComputationTime: Option[Long] = None
  private var realNumberOfRecordsValue: Option[Long] = None
  private var inputRecordsValue: Option[Int] = None
  private var isSelected: Boolean = false
  private var realSupportValue: Option[Double] = None

  def score: Score = scoreValue

  /**
   * Add the query result
   *
   * @param queryResult the query result
   */
  def addQueryResult(queryResult: Map[Map[String, Data[_]], QueryResultSimplified], executionTime: Long, inputRecords: Int): Unit = {
    result = Some(queryResult)
    queryComputationTime = Some(executionTime)
    realNumberOfRecordsValue = Some(queryResult.size)
    inputRecordsValue = Some(inputRecords)
    realSupportValue = Some(queryResult.toSeq.collect {
      case (dimensions, rx) if dimensions.forall(x => x._2 != NullData) => rx.N
    }.sum.toDouble / inputRecords)
  }

  def addQueryExecutionWithoutResult(numberOfResultRecords: Int, executionTime: Long, inputRecords: Int): Unit = {
    require(numberOfResultRecords >= 0, s"Number of records must be positive but is $numberOfResultRecords")
    queryComputationTime = Some(executionTime)
    realNumberOfRecordsValue = Some(numberOfResultRecords)
    inputRecordsValue = Some(inputRecords)
  }

  /**
   *
   * @return if the query has been selected */
  def setAsSelected(): Unit = isSelected = true

  /**
   *
   * @return if the query has been selected */
  def isSelectedQuery: Boolean = isSelected

  /**
   *
   * @return the query result if present
   */
  def getQueryResult: Option[Map[Map[String, Data[_]], QueryResultSimplified]] = result

  /**
   *
   * @return the real number of records if present
   */
  def realNumberOfRecords: Option[Long] = realNumberOfRecordsValue

  /**
   *
   * @return the execution time for the pane
   */
  def getExecutionTime: Option[Long] = queryComputationTime

  def isStored: Boolean = result.nonEmpty && isExecuted

  /**
   *
   * @return if the query has been executed
   */
  def isExecuted: Boolean = queryComputationTime.nonEmpty && realNumberOfRecordsValue.nonEmpty && inputRecordsValue.nonEmpty

  /**
   *
   * @return the number of computed records, if the query has been executed
   */
  def inputRecords: Option[Int] = inputRecordsValue

  def realSupport: Double = realSupportValue.getOrElse(0D)

  /**
   * Set the total score of the query for the naive execution considering the real support
   *
   * @param state the state
   */
  def changeTotalScoreForNaive(state: State): Unit = {
    require(state.configuration.isInstanceOf[NaiveConfiguration], "The configuration must be naive")
    scoreValue = scoreValue.updateSupports(state.getSupports(query,None))
  }
}