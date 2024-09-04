package it.unibo.big.streamanalysis.algorithm.generation.countdistinct

/**
 * The dimension statistics
 */
object DimensionStatistics {

  sealed trait Statistic {
    def support: Double
    def countD: Long
  }

  /**
   * The dimension statistics
   * @param support the support of the dimension
   * @param countD the count distinct of the dimension
   * @param isElected true if the dimension is elected during the queries generation in the current pane
   */
  case class DimensionStatistic(support: Double, countD: Long, isElected: Boolean) extends Statistic

  /**
   * The dimension vs dimension statistics
   * @param countD the count distinct of the pair of dimensions
   * @param support the support of the pair of dimensions
   */
  case class DimensionVsDimensionStatistic(support: Double, countD: Long) extends Statistic {
    private var functionalDependencyScoreValue: Option[Double] = None

    /**
     * Set the functional dependency score
     * @param score the score
     */
    def setFunctionalDependencyScore(score: Double): DimensionVsDimensionStatistic = {
      functionalDependencyScoreValue = Some(score)
      this
    }

    /**
     *
     * @return the functional dependency score
     */
    def functionalDependencyScore: Option[Double] = functionalDependencyScoreValue
  }
}
