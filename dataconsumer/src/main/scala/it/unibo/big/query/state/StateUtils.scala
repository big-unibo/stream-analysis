package it.unibo.big.query.state

import it.unibo.big.input.{AlgorithmConfiguration, NaiveConfiguration, StreamAnalysisConfiguration}

/**
 * Utils to configure the internal state
 */
object StateUtils {
  sealed trait StateType {
    /**
     * Set the state of the algorithm
     * @param gfsNaive the naive state
     * @param gfsMap the map of the other algorithm configurations' states
     * @param algorithmConfiguration the algorithm configuration
     * @return the algorithm state to use considering the algorithm configuration
     */
    def setState(gfsNaive: Option[State], gfsMap: Map[StreamAnalysisConfiguration, State], algorithmConfiguration: AlgorithmConfiguration): State

    /**
     * Get the extra path to use
     * @return the extra path
     */
    def extraPath: String
  }

  /**
   * Each algorithm have its own state
   */
  case object OwnState extends StateType {
    override def setState(gfsNaive: Option[State], gfsMap: Map[StreamAnalysisConfiguration, State], algorithmConfiguration: AlgorithmConfiguration): State =
      algorithmConfiguration match {
        case _: NaiveConfiguration if gfsNaive.isDefined => gfsNaive.get
        case x: StreamAnalysisConfiguration if gfsMap.contains(x) => gfsMap(x)
        case _ => throw new IllegalArgumentException("The configuration is not present")
      }

    override def extraPath: String = "default/"
  }

  /**
   * The starting state of each algorithm is the same of the naive algorithm
   */
  case object NaiveState extends StateType {
    override def setState(gfsNaive: Option[State], gfsMap: Map[StreamAnalysisConfiguration, State], algorithmConfiguration: AlgorithmConfiguration): State =
      algorithmConfiguration match {
        case x: StreamAnalysisConfiguration if gfsNaive.isDefined && gfsMap.contains(x) => gfsMap(x).setState(gfsNaive.get)
        case _: NaiveConfiguration if gfsNaive.isDefined => gfsNaive.get
        case _ => throw new IllegalArgumentException("The naive configuration is not present")
      }

    override def extraPath: String = "naive_state/"
  }

  /**
   * The starting state of each algorithm is the same of the one specified in the configuration
   * @param configuration the used StreamAnalysisConfiguration
   */
  case class OtherState(configuration: StreamAnalysisConfiguration) extends StateType {
    override def setState(gfsNaive: Option[State], gfsMap: Map[StreamAnalysisConfiguration, State], algorithmConfiguration: AlgorithmConfiguration): State =
      algorithmConfiguration match {
        case x: StreamAnalysisConfiguration if x == configuration && gfsMap.contains(configuration) => gfsMap(configuration)
        case x: StreamAnalysisConfiguration if gfsMap.contains(configuration) && gfsMap.contains(x) => gfsMap(x).setState(gfsMap(configuration))
        case _: NaiveConfiguration if gfsMap.contains(configuration) && gfsNaive.isDefined => gfsNaive.get.setState(gfsMap(configuration))
        case _ => throw new IllegalArgumentException("The configuration is not present")
      }

    override def extraPath: String = configuration match {
      case x: StreamAnalysisConfiguration if x.knapsack.isDefined => s"knapsack_log_factor${x.logFactor}/"
      case _ => s"log_factor_${configuration.logFactor}/"
    }
  }
}
