package it.unibo.big.input

/** Module of concept used for query the data */
object GPSJConcepts {

  import DataDefinition.{Data, NullData, NumericData, StringData}
  import GPSJConcepts.Operators.MeasureOperator

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

    case object Avg extends NotAggregableOperator {
      override def canBeComputedFrom(measure: String, preCoumputedResults: Map[Aggregation, Data[_]]): Boolean = {
        val aggSum = Aggregation(measure, Sum)
        val aggCount = Aggregation(measure, Count)
        if(preCoumputedResults.contains(aggSum) && preCoumputedResults.contains(aggCount)) {
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
            case NumericData(x) =>  NumericData(x.doubleValue()
              / preCoumputedResults(Aggregation(measure, Count)).asInstanceOf[NumericData].get)
          }
        } else {
          throw new IllegalArgumentException("Cannot compute avg")
        }
      }
    }

    case object Sum extends AggregableOperator {

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

    case object Min extends AggregableOperator {

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

    case object Max extends AggregableOperator {

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

      /**
       *
       * @param value the string representation of the aggregation operator
       * @return the aggregation operator
       */
      def fromString(value: String): Option[MeasureOperator] = aggregationFunctions.find(_.toString == value)
    }

    /*sealed trait SelectionOperator
    case object Equal extends SelectionOperator
    case object Greater extends SelectionOperator
    case object Lower extends SelectionOperator
    case object NotEqual extends SelectionOperator
    case object LowerEqual extends SelectionOperator
    case object GreaterEqual extends SelectionOperator
    object SelectionOperator {
      def fromString(value: String): Option[SelectionOperator] = Seq(Equal, Greater, Lower, NotEqual, LowerEqual, GreaterEqual).find(_.toString == value)
    }*/}

  object GPQuery {

    val queryRegex = "(.*)Query\\(Set\\((.*)\\),Set\\((.*)\\),Set\\((.*)\\)\\)(.*)".r

    def unapply(str: String): Option[GPQuery] = str match {
      case queryRegex(_, proj, aggr, _) => val px = split(proj, "Projection\\((\\d+)\\)", p => Projection(p group 1)).reverse.toSet

        //val sx = split(sel, "Selection\\((\\d+),(Equal|Greater|Lower|NotEqual|LowerEqual|GreaterEqual),(.*)\\)",
        //  s => Selection[Any]((s group 1).toInt, SelectionOperator.fromString(s group 2).get, s group 3)).toSet

        val ax = split(aggr, "Aggregation\\((\\d+),(.*)\\)", a => Aggregation(a group 1, MeasureOperator fromString (a group 2) get)).reverse.toSet

        Some(GPQuery(px, ax))
      case _ => None
    }

    import scala.util.matching.Regex

    private def split[T](str: String, regex: String, f: Regex.Match => T): Seq[T] = {
      if ((regex.r findFirstMatchIn str).toList.isEmpty) Seq() else str.split(", ").map(v => f(regex.r.findFirstMatchIn(v).get))
    }
  }
}
