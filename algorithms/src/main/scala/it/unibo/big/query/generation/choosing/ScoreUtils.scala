package it.unibo.big.query.generation.choosing

import it.unibo.big.input.AlgorithmConfiguration
import it.unibo.big.input.GPSJConcepts.GPQuery
import it.unibo.big.query.generation.QueryUtils.{QueriesWithStatistics, getFdScore}
import it.unibo.big.query.state.{QueryStatisticsInPane, State}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Utility object to handle the scores
 */
object ScoreUtils {

  /**
   * Define a case trait to hold the dimension score
   */
  sealed trait DimensionScore {
    /**
     * The query dimension
     */
    val dim: String

    /**
     * The score of the dimension
     */
    val score: Double
  }

  /**
   * Define a case class to hold the assignment score
   * @param dim1 the first dimension (referred to the input query)
   * @param dim2 the second dimension (referred to actual query dimension)
   * @param score the score of the assignment
   */
  case class AssignmentScore(dim1: String, dim2: String, score: Double) extends DimensionScore {
    override def toString: String = s"($dim1, $dim2) -> ${score.formatted("%.2f")}"

    override val dim: String = dim2
  }

  /**
   * Define a case class to hold the score in the pane
   * @param fdsScore the score of the functional dependencies
   * @param supportScore the score of the support
   * @param lastPaneSupport the support of the query in the last pane
   * @param paneTime the pane time
   * @param configuration the configuration
   */
  case class Score(private var fdsScore: Double, private var supportScore: Double, private var lastPaneSupport: Double, paneTime: Long, configuration: AlgorithmConfiguration) {
    checkValues(fdsScore, supportScore, lastPaneSupport)
    private var valueTmp: Double = 0D
    calculateValue()

    private def calculateValue() : Unit = {
      valueTmp = configuration.alpha * supportScore + configuration.beta * fdsScore
    }


    private def checkValues(totalFDS: Double, totalSupport: Double, lastPaneSupport: Double) : Unit = {
      require(totalFDS >= 0 && totalFDS <= 1, "the value of functional dependencies must be in [0,1]")
      require(totalSupport >= 0 && totalSupport <= 1, s"the value of support must be in [0,1] but is $totalSupport")
      require(lastPaneSupport >= 0 && lastPaneSupport <= 1, s"the value of support must be in [0,1] but is $lastPaneSupport")
    }

    def updateSupports(newSupports: Map[Long, Double]): Score = {
      supportScore = Score.getWeightedValuesForLastPane(newSupports, paneTime)
      lastPaneSupport = newSupports(paneTime)
      checkValues(fdsScore, supportScore, lastPaneSupport)
      calculateValue()
      this
    }
    /**
     *
     * @return the score value
     */
    def value: Double = valueTmp

    def fdValue: Double = fdsScore

    def supportValue: Double = supportScore

    def paneSupport : Double = lastPaneSupport
  }

  object Score {

    private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

    /**
     *
     * @param values the values
     * @param paneTime the considered time
     * @param lastPaneWeight the weight of the last pane
     * @return the weighted value considering the last pane
     */
    private def getWeightedValuesForLastPane(values: Map[Long, Double], paneTime: Long, lastPaneWeight : Double = 0.5): Double = {
      val otherPanes = values.filterKeys(_ != paneTime)
      val lastPaneValue = if(otherPanes.isEmpty) 0D else util.Try(otherPanes.values.sum / otherPanes.size).toOption.getOrElse(0D)
      lastPaneWeight * values.getOrElse(paneTime, 0D) + (1 - lastPaneWeight) * lastPaneValue
    }

    /**
     * Calculate the score of the query
     * @param algorithmState the algorithm state
     * @param inputQuery the input query
     * @param paneTime the pane time
     * @param q the query
     * @param support the support of the query in the last pane
     * @param gamma the gamma value for weight fds
     * @param queryCardinalities the cardinalities of the queries in each pane
     * @param fdLastPane the functional dependencies of the last pane (at paneTime), if not provided is taken from the status (if present)
     * @return the score of the query
     */
    def apply(algorithmState: State, inputQuery: Option[GPQuery], paneTime: Long, q: GPQuery, support: Option[Double], queryCardinalities: Map[Long, Map[GPQuery, Long]], fdLastPane: Map[(String, String), Double], gamma: Double = 0.8): Score = {
      val configuration = algorithmState.configuration
      //iterate throw all times in the state to obtain the score
      val stateFD = algorithmState.getFDSForEachTimeStamp
      val fdMap : Map[Long, Map[(String, String), Double]] = stateFD + (paneTime -> stateFD.getOrElse(paneTime, fdLastPane))
      val fdsState = fdMap.mapValues(getFdScore(inputQuery, q, _)).collect{
        case (t, scores) if scores.nonEmpty =>
          val averageFDS = scores.toSeq.map(_.score).sum / scores.size
          val cardinalityQ = util.Try(queryCardinalities(t)(q)).toOption
          val cardinalityInput = util.Try(queryCardinalities(t)(inputQuery.get)).toOption
          if(cardinalityInput.isDefined && cardinalityQ.isDefined) {
            var queryCompatibility = List(cardinalityInput.get.toDouble / cardinalityQ.get.toDouble, cardinalityQ.get.toDouble / cardinalityInput.get.toDouble)
              .map(x => if(x.isNaN || x.isInfinity) 0D else x).min
            if(queryCompatibility > 1) {
              LOGGER.warn(s"queryCompatibility is higher than 1 for $inputQuery and $q with countd q = $cardinalityQ countd input = $cardinalityInput, setting to 1")
              queryCompatibility = 1
            }
            require(queryCompatibility >= 0 && queryCompatibility <= 1, s"queryCompatibility must be in [0,1] but is $queryCompatibility")
            t -> Some(gamma * averageFDS + (1 - gamma) * queryCompatibility)
          } else {
            t -> None
          }
      }.filter(_._2.nonEmpty).map(x => x._1 -> x._2.get)

      var stateSupports = algorithmState.getSupports(q, Some(paneTime))
      if(support.isDefined) { stateSupports += (paneTime -> support.get) }
      Score(Score.getWeightedValuesForLastPane(fdsState, paneTime), Score.getWeightedValuesForLastPane(stateSupports, paneTime), stateSupports(paneTime), paneTime, configuration)
    }
  }

  /**
   * Sort the queries by total score (descending) and number of estimated number of records (ascending)
   * @param queriesWithTotalScore the map of queries with SOP
   * @param scoringFactor a function that for a given query return the first scoring factor (default is equal for every query to 0)
   * @return the sorted queries with SOP
   */
  def sortQueries(queriesWithTotalScore: QueriesWithStatistics, scoringFactor: GPQuery => Int = _ => 0): Seq[(GPQuery, QueryStatisticsInPane)] = {
    queriesWithTotalScore.toSeq.sortBy {
      case (q, QueryStatisticsInPane(_, estimatedRecords, totalScore, _, _)) => (scoringFactor(q), -totalScore.value, estimatedRecords, q.dimensions.toSeq.sorted.mkString(",")) //sort by total score (descending) and estimated number of records (ascending)
    }
  }

  /**
   * Get the best query to execute
   * @param paneTime the pane time
   * @param algorithmState the algorithm state
   * @param inputQuery the input query
   * @param firstExecutedQuery the first executed query with its score
   * @return the selected query with its statistics
   */
  def getBestQuery(paneTime: Long, algorithmState: State, inputQuery: Option[GPQuery], firstExecutedQuery: Option[(GPQuery, QueryStatisticsInPane)]): Option[(GPQuery, QueryStatisticsInPane)] = {
    inputQuery.flatMap { query =>
      val is = algorithmState.getQueries(paneTime)(query)
      firstExecutedQuery match {
        case Some((_, qs)) if qs.score.value > is.score.value =>
          //if the input query is executed and the first executed query has a higher score, we set it as selected
          qs.setAsSelected()
          firstExecutedQuery
        case _ =>
          //if the input query still the one with best score, we set it as selected
          is.setAsSelected()
          Some(query, is)
      }
    }.orElse {
      firstExecutedQuery.foreach { case (_, qs) =>
        //if the input query is not defined, we set the first executed query as selected
        qs.setAsSelected()
      }
      firstExecutedQuery
    }
  }
}
