package it.unibo.big.streamanalysis.algorithm.state

import it.unibo.big.streamanalysis.input.{AlgorithmConfiguration, NaiveConfiguration, StreamAnalysisConfiguration}

/**
 * Utils to configure the internal algorithm' state
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
}
