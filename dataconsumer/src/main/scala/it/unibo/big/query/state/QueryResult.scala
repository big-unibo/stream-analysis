package it.unibo.big.query.state

import it.unibo.big.input.DataDefinition.Data
import it.unibo.big.input.GPSJConcepts.Aggregation
import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.query.state.StateDefinitionUtils.{aggregateMeasures, getStartingMeasuresValues}

/**
 *
 * @param timestamp the last timestamp
 * @param dataTmp data in the query result
 */
abstract class QueryResult(timestamp: Long, private var dataTmp: Seq[Record]) {
  import scala.runtime.ScalaRunTime

  /**
   * Initial dimension of the query result
   */
  def initialSize: Int

  /**
   * Initial map of measures and values for each aggregation operator
   *
   * @return map of measures and values for each aggregation operator
   */
  def startingMeasuresValues : Map[Aggregation, Data[_]]

  /**
   * Dimensions of the query result
   * @return the dimensions of the query result map String and value
   */
  def dimensions: Map[String, Data[_]]

  private var numberOfElements: Int = initialSize
  private var measuresValues: Map[Aggregation, Data[_]] = startingMeasuresValues
  private var lastTimeStamp = timestamp

  /**
   *
   * @return records that are active in the query result, for debug the result
   */
  def data: Seq[Record] = dataTmp

  /**
   * @return number of element of the query result
   */
  def N: Int = numberOfElements

  /**
   * @return measures of the query result
   */

  def measures:  Map[Aggregation, Data[_]] = measuresValues

  /**
   * @return the most recent timestamp of the query result
   */
  def t: Long = lastTimeStamp
  /**
   * @param s the schema to add
   * @param t schema timestamp
   *          Update the query result adding s
   */
  def add(s: Record, t: Long): Unit = {
    require(dimensions.forall(d => s.data(d._1) == d._2))
    numberOfElements += 1
    measuresValues = aggregateMeasures(measures, getStartingMeasuresValues(measures.keySet, Seq(s)))
    lastTimeStamp = math.max(t, lastTimeStamp)
    dataTmp :+= s
  }

  /**
   * @param queryResult the query result to add
   *           Update the query result adding queryResult, updating also the schemas
   */
  def add[T <: QueryResult](queryResult: QueryResult): T = {
    dataTmp ++= queryResult.data
    addWithoutData(queryResult)
  }

  /**
   *
   * @param queryResult the query result to add
   *           Update the query result adding queryResult
   * @tparam T the type to return
   * @return update the state without adding raw records
   */
  def addWithoutData[T <: QueryResult](queryResult: QueryResult): T = {
    require(dimensions == queryResult.dimensions)
    measuresValues = aggregateMeasures(measures, queryResult.measures)
    numberOfElements += queryResult.N
    lastTimeStamp = math.max(lastTimeStamp, queryResult.t)
    this.asInstanceOf[T]
  }

  override def equals(obj: Any): Boolean = obj match {
    case x: QueryResult if x.dimensions == dimensions && x.measures == measures && x.N == N && x.t == t => true
    case _ => false
  }

  override def hashCode(): Int = ScalaRunTime._hashCode((dimensions, measures, N, t))

}

