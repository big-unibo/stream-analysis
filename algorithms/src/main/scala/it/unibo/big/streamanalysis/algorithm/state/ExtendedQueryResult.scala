package it.unibo.big.streamanalysis.algorithm.state

import it.unibo.big.streamanalysis.input.DataDefinition.{Data, NullData, NumericData}
import it.unibo.big.streamanalysis.input.GPSJConcepts.Operators._
import it.unibo.big.streamanalysis.input.GPSJConcepts.{Aggregation, GPQuery}
import it.unibo.big.streamanalysis.input.RecordModeling.Record
import StateDefinitionUtils.getStartingMeasuresValues
import it.unibo.big.streamanalysis.input.DataDefinition.{Data, NullData}
import it.unibo.big.streamanalysis.input.GPSJConcepts.Operators.{AggregableOperator, Count, NotAggregableOperator}
import it.unibo.big.streamanalysis.input.GPSJConcepts.{Aggregation, GPQuery}
import it.unibo.big.streamanalysis.input.RecordModeling.Record

object StateDefinitionUtils {
  /**
   * Internal state of the algorithm
   */
  type InternalState = Map[Long, Map[GPQuery, QueryStatisticsInPane]]

  /**
   * Aggregate the measures of two results
   *
   * @param measures1 the first result measures
   * @param measures2 the second result measures
   * @return the aggregated measures
   */
  def aggregateMeasures(measures1: Map[Aggregation, Data[_]], measures2: Map[Aggregation, Data[_]]): Map[Aggregation, Data[_]] = {
    val measures = measures1.keySet ++ measures2.keySet
    val aggregationResult = measures.collect {
      case Aggregation(k, operator: AggregableOperator) =>
        val notPresentValue = operator match {
          case Count => NumericData(0)
          case _ => NullData
        }
        Aggregation(k, operator) -> operator.aggregate(measures1.getOrElse[Data[_]](Aggregation(k, operator), notPresentValue), measures2.getOrElse[Data[_]](Aggregation(k, operator), notPresentValue))
    }.toMap[Aggregation, Data[_]]

    aggregationResult ++ getNotAggregableRecordsResults(measures, aggregationResult)
  }

  def getStartingMeasuresValues(aggregations: Set[Aggregation], dataTmp: Seq[Record]): Map[Aggregation, Data[_]] = {
    val aggregationsMap: Map[String, Set[Aggregation]] = aggregations.map(a => a.measure -> a).groupBy(_._1).mapValues(_.map(_._2))

    val aggregationResult = dataTmp.flatMap(d => aggregationsMap.keys.map(m => m -> d.data.getOrElse[Data[_]](m, NullData)))
      .groupBy(_._1).flatMap {
        case (m, vx) =>
          val data = vx.map(_._2)
          aggregationsMap(m).collect{
            case a if a.aggregationOperator.isInstanceOf[AggregableOperator] => a.aggregationOperator match {
              case Count => a -> NumericData(data.size)
              case op: AggregableOperator => a -> data.reduce(op.aggregate)
            }
          }
      }.toMap[Aggregation, Data[_]]

    aggregationResult ++ getNotAggregableRecordsResults(aggregations, aggregationResult)
  }

  /**
   *
   * @param measures the measures to aggregate
   * @param aggregationResult the partial aggregation result
   * @return the not aggregate measures results (for the mean calculation)
   */
  private def getNotAggregableRecordsResults(measures: Set[Aggregation], aggregationResult: Map[Aggregation, Data[_]]): Map[Aggregation, Data[_]] = {
    measures.collect {
      case Aggregation(measure, op: NotAggregableOperator) =>
        Aggregation(measure, op) -> op.compute(measure, aggregationResult)
    }.toMap[Aggregation, Data[_]]
  }

  /**
   * @param query      the query
   * @param dimensions the dimensions of the query result
   * @param ps         input queries results
   * @return the aggregated query results
   * */
  def aggregateQueryResults(query: GPQuery, dimensions: Map[String, Data[_]], ps: Seq[QueryResultSimplified]): QueryResultAggregator = {
    var reduceQueryResult: QueryResultAggregator = QueryResultAggregator(dimensions, query)
    for (p <- ps) {
      reduceQueryResult = reduceQueryResult.add(p)
    }
    reduceQueryResult
  }

  /**
   * Class for group results in a pane.
   *
   * @param dimensions the dimensions of the query result
   * @param query      the query
   */
  case class QueryResultAggregator(override val dimensions: Map[String, Data[_]], query: GPQuery) extends QueryResult(0L, Seq()) {

    override lazy val initialSize: Int = 0
    override lazy val startingMeasuresValues: Map[Aggregation, Data[_]] = query.aggregations.map(a => a -> NullData).toMap
    private var panesData: Map[Long, QueryResult] = Map()

    override def add(s: Record, t: Long): Unit = throw new UnsupportedOperationException()

    override def data: Seq[Record] = panesData.flatMap(_._2.data).toSeq

    /**
     *
     * @param queryResult the query result to add
     *            Update the query result adding queryResult, works only if the dimensions are the same and the timestamp is not already present
     * @tparam GF the other gf type
     * @return the merged gfs */
    override def add[GF <: QueryResult](queryResult: QueryResult): GF = {
      queryResult match {
        case y: QueryResultSimplified if y.dimensions == dimensions && !panesData.keySet.contains(y.timestamp) => panesData += y.timestamp -> y
      }
      super.addWithoutData(queryResult) //use simplified method and avoid to compute add of schemas
    }

  }
}

case class AlgorithmExecutionTimeStatistics(private var timeForUpdateWindow: Long = 0,
                                            private var timeForComputeQueryInTheWindow: Long = 0,
                                            private var timeForGettingScores: Long = 0,
                                            private var timeForUpdateThePane: Long = 0,
                                            private var timeForScoreComputation: Long = 0,
                                            private var timeForChooseQueries: Long = 0,
                                            private var timeForQueryExecution: Long = 0
                                           ) {
  def getTimeForUpdateWindow: Long = timeForUpdateWindow
  def addTimeForUpdateWindow(timeForUpdateWindow: Long): Unit = this.timeForUpdateWindow += timeForUpdateWindow

  def getTimeForComputeQueryInTheWindow: Long = timeForComputeQueryInTheWindow
  def addTimeForComputeQueryInTheWindow(timeForComputeQueryInTheWindow: Long): Unit = this.timeForComputeQueryInTheWindow += timeForComputeQueryInTheWindow

  def getTimeForGettingScores: Long = timeForGettingScores
  def addTimeForGettingScores(timeForGettingScores: Long): Unit = this.timeForGettingScores += timeForGettingScores

  def getTimeForUpdateThePane: Long = timeForUpdateThePane
  def addTimeForUpdateThePane(timeForUpdateThePane: Long): Unit = this.timeForUpdateThePane += timeForUpdateThePane

  def getTimeForScoreComputation: Long = timeForScoreComputation
  def addTimeForScoreComputation(timeForScoreComputation: Long): Unit = this.timeForScoreComputation += timeForScoreComputation

  def getTimeForChooseQueries: Long = timeForChooseQueries
  def addTimeForChooseQueries(timeForChooseQueries: Long): Unit = this.timeForChooseQueries += timeForChooseQueries

  def getTimeForQueryExecution: Long = timeForQueryExecution
  def addTimeForQueryExecution(timeForQueryExecution: Long): Unit = this.timeForQueryExecution += timeForQueryExecution

  def reset() : Unit = {
    timeForUpdateWindow = 0
    timeForComputeQueryInTheWindow = 0
    timeForGettingScores = 0
    timeForUpdateThePane = 0
    timeForScoreComputation = 0
    timeForChooseQueries = 0
    timeForQueryExecution = 0
  }
}

/**
 * Class for query result in a pane.
 *
 * @param timestamp the last timestamp
 * @param query     the used query
 * @param dimensions the query dimensions
 * */
case class QueryResultSimplified(timestamp: Long, query: GPQuery, dimensions: Map[String, Data[_]], private var dataTmp: Seq[Record]) extends QueryResult(timestamp, dataTmp) {
  override lazy val initialSize: Int = dataTmp.size

  require(query.dimensions == dimensions.keySet)

  override lazy val startingMeasuresValues: Map[Aggregation, Data[_]] = getStartingMeasuresValues(query.aggregations, dataTmp)
}
