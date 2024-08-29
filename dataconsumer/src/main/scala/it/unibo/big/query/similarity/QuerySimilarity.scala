package it.unibo.big.query.similarity

import org.slf4j.{Logger, LoggerFactory}

/**
 * Query similarity computation
 */
object QuerySimilarity {

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  //define the hierarchies
  private val hierarchies = Array(
    Array(
      "activity_timestamp_ms",
      "activity_timestamp_s",
      "activity_timestamp_min",
      "activity_timestamp_hour",
      "activity_timestamp_day",
      "activity_timestamp_month",
      "activity_timestamp_year",
      "activity_timestamp_all"
    ),
    Array(
      "farm",
      "farm_creation_date_day",
      "farm_creation_date_month",
      "farm_creation_date_year",
      "farm_date_all"
    ),
    Array(
      "farm",
      "area",
      "locality",
      "region",
      "country",
      "farm_locality_all"
    ),
    Array(
      "farm",
      "company",
      "farm_company_all"
    ),
    Array(
      "robot",
      "model",
      "robot_all"
    ),
    Array(
      "task",
      "goal",
      "task_goal_all"
    ),
    Array(
      "task",
      "status",
      "task_status_all"
    )
  )

  /**
   * Get the hierarchies for the dimensions of a query
   * @param dimensions the dimensions
   * @param hierarchies the hierarchies used in the query
   * @return a map with minimal set of hierarchies and reference dimension for the query (the less aggregated)
   */
  private def getHierarchies(dimensions: Set[String], hierarchies: Array[Array[String]]): Map[Array[String], String] = {
    //if not exists a hierarchy suppose the field is of a single new hierarchy
    dimensions.flatMap {
      case d if hierarchies.exists(h => h.contains(d)) =>
        hierarchies.filter(_.contains(d)).map(h => h -> d)
    }.groupBy(_._1).map{
      case (h, d) => h -> d.minBy(d => d._1.indexOf(d._2))._2
    }
  }

  /**
   * Compute the similarity between two queries
   * @param dimensionsQ1 the dimensions of the first query
   * @param dimensionsQ2 the dimensions of the second query
   * @return the similarity [0,1]
   */
  def compute(dimensionsQ1: Set[String], dimensionsQ2: Set[String]): Double = {
    require(dimensionsQ1.size == dimensionsQ2.size)
    LOGGER.info(s"Computing similarity between $dimensionsQ1 and $dimensionsQ2")
    var usedHierarchies = hierarchies
    if((dimensionsQ1 ++ dimensionsQ2).exists(d => !hierarchies.flatten.contains(d))) {
      LOGGER.warn(s"Dimensions not in the hierarchies: ${dimensionsQ1 ++ dimensionsQ2}, consider each dimension as an hierarchy")
      usedHierarchies = (dimensionsQ1 ++ dimensionsQ2).map(d => Array(d, s"${d}_all")).toArray
    }
    //get all the hierarchies for the dimensions
    val dimQ1HierarchiesMap = getHierarchies(dimensionsQ1, usedHierarchies)
    val dimQ2HierarchiesMap = getHierarchies(dimensionsQ2, usedHierarchies)

    val distances = (dimQ1HierarchiesMap.keys ++ dimQ2HierarchiesMap.keys).map{ h =>
        val d1 = dimQ1HierarchiesMap.getOrElse(h, h.last)
        val d2 = dimQ2HierarchiesMap.getOrElse(h, h.last)
        (d1, d2) -> computeDistance(d1, d2, h)
      }.toMap

    distances.foreach(d => LOGGER.info(s"Distance between ${d._1._1} and ${d._1._2} = ${d._2}"))
    //calculate the similarity using the distances
    val similarity = 1 - (distances.values.sum / distances.size)
    LOGGER.info(s"Similarity = $similarity Previous query = ${dimensionsQ1.mkString(",")} -- query = ${dimensionsQ2.mkString(",")}")
    require(similarity >= 0 && similarity <= 1, s"Similarity not in [0,1]: $similarity")
    similarity
  }

  /**
   * Compute the distance between two dimensions
   *
   * @param dim1 the first dimension
   * @param dim2 the second dimension
   * @param h the hierarchy where the dimensions are
   * @return the distance between the two dimensions, if they are comparable (i.e., they are in the same hierarchy)
   */
  private def computeDistance(dim1: String, dim2: String, h: Array[String]): Double = {
    require(h.contains(dim1) && h.contains(dim2), s"Dimensions $dim1 and $dim2 are not in the same hierarchy: ${h.mkString(",")}")
    (dim1, dim2) match {
      case (d1, d2) => (h.indexOf(d1) - h.indexOf(d2)).abs.toDouble / (h.length - 1) //take the hierarchy where the distance is minimum
    }
  }

  /*val dimensions = hierarchies.flatten.filter(x => !x.endsWith("_all"))
  val queries = dimensions.flatMap(h => dimensions.filter(_ != h).map(h2 => Set(h, h2))).toSet
  queries.map(q => queries.map(q2 => compute(q, q2)))*/
}
