package it.unibo.big.streamanalysis.input

/** Module of concept used for query the data */
object GPSJConcepts {

  import DataDefinition.{Data, NullData, NumericData, StringData}

  import scala.language.postfixOps

  /**
   * Aggregation class
   * @param measure the measure to aggregate
   * @param aggregationOperator the aggregation operator
   */
  case class Aggregation(measure: String, aggregationOperator: Operators.MeasureOperator) {
    override def toString: String = s"$aggregationOperator($measure)"
  }

  //case class Selection[T](attributeName: String, selectionOperator: Operators.SelectionOperator, value: T)

  /**
   * Projection class
   * @param dimension the dimension to project
   */
  case class Projection(dimension: String)

  /**
   * Query class
   * @param projections list of projections
   * @param aggregations list of aggregations
   */
  case class GPQuery(projections: Set[Projection], aggregations: Set[Aggregation] = Set.empty) {
    /**
     *
     * @return the dimensions of the query, as the set of the dimensions of the projections
     */
    def dimensions: Set[String] = projections.map(_.dimension)

    /**
     *
     * @return the measures of the query, as the set of the measures of the aggregations
     */
    def measures: Set[String] = aggregations.map(_.measure)

    override def equals(obj: Any): Boolean = obj match {
      case q: GPQuery => q.dimensions == dimensions
      case _ => false
    }

    override def hashCode(): Int = dimensions.hashCode()
  }

  /**
   * Query pattern class
   * @param numberOfDimensions the number of dimensions of the query
   * @param maxRecords the maximum number of records to return, if specified the query is a top-k query
   */
  case class QueryPattern(numberOfDimensions: Int, maxRecords: Option[Int])

  object Operators {
    /**
     * Aggregation operator
     */
    sealed trait MeasureOperator

    sealed trait AggregableOperator extends MeasureOperator {
      /**
       *
       * @param v1 the value to aggregate
       * @param v2 the value to aggregate
       * @return the aggregation of the values
       */
      def aggregate(v1: Data[_], v2: Data[_]): Data[_]
    }


    sealed trait NotAggregableOperator extends MeasureOperator {

      def canBeComputedFrom(measure: String, preCoumputedResults: Map[Aggregation, Data[_]]): Boolean

      def compute(measure: String, preCoumputedResults: Map[Aggregation, Data[_]]): Data[_]
    }

    private case object Avg extends NotAggregableOperator {
      override def canBeComputedFrom(measure: String, preCoumputedResults: Map[Aggregation, Data[_]]): Boolean = {
        val aggSum = Aggregation(measure, Sum)
        val aggCount = Aggregation(measure, Count)
        if (preCoumputedResults.contains(aggSum) && preCoumputedResults.contains(aggCount)) {
          (preCoumputedResults(aggSum).isInstanceOf[NumericData] || preCoumputedResults(aggSum) == NullData) &&
            preCoumputedResults(aggCount).isInstanceOf[NumericData]
        } else {
          false
        }
      }

      override def compute(measure: String, preCoumputedResults: Map[Aggregation, Data[_]]): Data[_] = {
        if (canBeComputedFrom(measure, preCoumputedResults)) {
          preCoumputedResults(Aggregation(measure, Sum)) match {
            case NullData => NullData
            case NumericData(x) => NumericData(x.doubleValue()
              / preCoumputedResults(Aggregation(measure, Count)).asInstanceOf[NumericData].get)
          }
        } else {
          throw new IllegalArgumentException("Cannot compute avg")
        }
      }
    }

    private case object Sum extends AggregableOperator {

      override def aggregate(v1: Data[_], v2: Data[_]): Data[_] = {
        (v1, v2) match {
          case (NumericData(x), NumericData(y)) => NumericData(x.doubleValue() + y.doubleValue())
          case (StringData(x), StringData(y)) => StringData(x.concat(y))
          case (NullData, NumericData(y)) => NumericData(y)
          case (NumericData(x), NullData) => NumericData(x)
          case (NullData, StringData(y)) => StringData(y)
          case (StringData(x), NullData) => StringData(x)
          case (NullData, NullData) => NullData
          case _ => throw new IllegalArgumentException("Sum not supported for type")
        }
      }
    }

    private case object Min extends AggregableOperator {

      override def aggregate(v1: Data[_], v2: Data[_]): Data[_] = {
        (v1, v2) match {
          case (NumericData(x), NumericData(y)) => NumericData(x.doubleValue().min(y.doubleValue()))
          case (NullData, NumericData(y)) => NumericData(y)
          case (NumericData(x), NullData) => NumericData(x)
          case (NullData, NullData) => NullData
          case _ => throw new IllegalArgumentException("Sum not supported for type")
        }
      }
    }

    private case object Max extends AggregableOperator {

      override def aggregate(v1: Data[_], v2: Data[_]): Data[_] = {
        (v1, v2) match {
          case (NumericData(x), NumericData(y)) => NumericData(x.doubleValue().max(y.doubleValue()))
          case (NullData, NumericData(y)) => NumericData(y)
          case (NumericData(x), NullData) => NumericData(x)
          case (NullData, NullData) => NullData
          case _ => throw new IllegalArgumentException("Sum not supported for type")
        }
      }
    }

    case object Count extends AggregableOperator {
      override def aggregate(v1: Data[_], v2: Data[_]): Data[_] = Sum.aggregate(v1, v2)
    }

    object MeasureOperator {

      /**
       *
       * @return the list of aggregation functions
       */
      def aggregationFunctions: Seq[MeasureOperator] = Seq(Sum, Min, Max, Count, Avg)
    }
  }
}
