package it.unibo.big.query.generation.functionaldependencies

import breeze.optimize.linear.KuhnMunkres
import it.unibo.big.query.generation.choosing.ScoreUtils.AssignmentScore
import org.slf4j.{Logger, LoggerFactory}

/**
* Matcher is any object that solves an assignment problem. The problem consists of finding
* a maximum cost matching (or a minimum cost perfect matching) in a bipartite graph. The input
  * graph is usually represented as a cost matrix. Zero values define the absence of edges.
  *
* === General Formulation ===
*
* Each problem instance has a number of agents and a number of tasks. Any agent can be assigned to
  * any task, incurring a cost that may vary depending on the agent-task assignment. It is required
* that all tasks are assigned to exactly one agent in such a way that the total cost is minimized.
* In case the numbers of agents and tasks are equal and the total cost of the assignment for all tasks
  * is equal to the sum of the costs for each agent then the problem is called the linear assignment problem.
  *
* @see https://en.wikipedia.org/wiki/Assignment_problem
  */
// Modify the HungarianMatcher to accept a list of AssignmentScore and return the best assignment
object HungarianMatcher {

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * @param assignmentScores the map of AssignmentScore
   * @return the used score solving the assignment problem by using the Hungarian algorithm
   */
  def solve(assignmentScores: Map[(String, String), AssignmentScore]): Set[AssignmentScore] = {
    val scores = assignmentScores.values.toSeq
    val distinctDims1 = assignmentScores.keys.map(_._1).toList
    val distinctDims2 = assignmentScores.keys.map(_._2).toList
    val dim1Map = distinctDims1.zipWithIndex.toMap
    val dim2Map = distinctDims2.zipWithIndex.toMap

    val costMatrix = Array.fill(distinctDims1.size, distinctDims2.size)(Double.MaxValue)

    scores.foreach { g =>
      val i = dim1Map(g.dim1)
      val j = dim2Map(g.dim2)
      require(g.score >= 0)
      costMatrix(i)(j) = g.score * -1 // negate the score because we want to maximize the cost
    }

    val inputMatrix = costMatrix.map(_.toSeq)
    val (matches, cost) = KuhnMunkres.extractMatching(inputMatrix)
    val score = cost * -1 // negate the cost to get the maximum cost

    val usedScores = matches.zipWithIndex.map {
      case (i, iIn) =>
        LOGGER.debug(s"${distinctDims1(iIn)} becomes ${distinctDims2(i)} (score = ${assignmentScores((distinctDims1(iIn), distinctDims2(i))).score})")
        assignmentScores((distinctDims1(iIn), distinctDims2(i)))
    }.toSet

    LOGGER.debug(s"Query score for the assignment $score")
    usedScores
  }
}
