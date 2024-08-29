package it.unibo.big.query.app

import it.unibo.big.input.GPSJConcepts.QueryPattern
import it.unibo.big.input._
import it.unibo.big.query.app.DatasetsUtils._
import it.unibo.big.query.app.ExecuteTests._
import it.unibo.big.query.execution.QueryExecutionTimeUtils

object FullDatasetsTestPaper {
  /**
   * Apply the full datasets test paper
   * @param executionConfigurations the execution configurations
   * @param datasets the datasets to execute the tests
   * @param numberOfRecordsPane the number of records per pane
   * @param numberOfPanes the number of panes
   * @param numberOfWindowsToConsider the number of windows to consider
   * @param queryExecutionTime the query execution time
   * @param frequency the frequency of records in ms
   */
  def apply(executionConfigurations: Seq[ExecutionConfiguration], datasets: Seq[Dataset], numberOfRecordsPane: Long = 10000L, numberOfPanes: Int = 5,
            queryExecutionTime : (SimulationConfiguration, Int, Long) => Long = QueryExecutionTimeUtils.getExecutionTime,
            numberOfWindowsToConsider: Dataset => Int = d => if(d.isInstanceOf[ChangingSyntheticDataset]) 30 else 15, frequency: Double = 10): Unit = {

    require(executionConfigurations.nonEmpty, "Execution configurations must be non empty")

    val configurations: Map[ConfigurationSetting, (Option[NaiveConfiguration], Seq[StreamAnalysisConfiguration])] = executionConfigurations.map{
      case ExecutionConfiguration(alpha, groupBySet, maxRecordsStatePercentage, maxRecordsQueries, algorithms) =>
          val logFactors = maxRecordsStatePercentage.toSeq.map(p => {
            p -> ConfigurationUtils.getLogFactor(p, numberOfRecordsPane.toInt)
          }).toMap
          val ourExecutionAlgorithms = algorithms.collect{
            case x if x.isInstanceOf[OurExecutionAlgorithm] => x.asInstanceOf[OurExecutionAlgorithm]
          }
          val qp = QueryPattern(groupBySet, None)
          val usedLogFactors = logFactors.filterKeys(_ >= maxRecordsQueries).values.toSet
          if(ourExecutionAlgorithms.nonEmpty) {
            require(usedLogFactors.nonEmpty, s"Log factor not found for $maxRecordsQueries")
          }
          val setting = ConfigurationSetting(alpha, qp, maxRecordsQueries)
          val naiveConfiguration = if(algorithms.contains(Naive)) Some(NaiveConfiguration(qp, alpha, maxRecordsQueries)) else None
          val algorithmsConfigurations = if(ourExecutionAlgorithms.nonEmpty) StreamAnalysisConfiguration.getConfigurations(
            queryExecutionTime = queryExecutionTime,
            queryPattern = qp,
            alphas = Set(alpha),
            logFactors = usedLogFactors,
            algorithms = ourExecutionAlgorithms,
            getPercentage = _ => maxRecordsQueries
          ) else Seq()
          setting -> (naiveConfiguration, algorithmsConfigurations)
    }.groupBy(_._1).mapValues(xs => (util.Try(xs.map(_._2._1).filter(_.nonEmpty).head.get).toOption, xs.flatMap(_._2._2)))
    ExecuteTests(_ => configurations, numberOfWindowsToConsider, numberOfPanes, slideDuration = numberOfRecordsPane, datasets = datasets, availableTime = math.ceil(numberOfRecordsPane / frequency).toLong)
  }
}

/**
 * Execution configuration
 * @param alpha the alpha
 * @param groupBySet  the group by set of queries
 * @param maxRecordsStatePercentage the max records state percentage
 * @param maxRecordsQueries the max records queries
 * @param algorithms to execute
 */
case class ExecutionConfiguration(alpha: Double, groupBySet: Int, maxRecordsStatePercentage: Set[Double], maxRecordsQueries: Double,
                                  algorithms: Set[ExecutionAlgorithm]) {
  require(alpha >= 0 && alpha <= 1, "Alpha must be in [0, 1]")
  require(groupBySet > 0, "Group by set must be positive")
  if(maxRecordsStatePercentage.isEmpty) {
    require(!algorithms.exists(_.isInstanceOf[OurExecutionAlgorithm]), "If no max records state percentage is defined, only naive algorithm must be executed")
  }
  require(maxRecordsQueries >= 0, "Max records queries must be positive")
  require(maxRecordsStatePercentage.forall(x => x > 0 && x <= 1), "Max records state percentage must be between ]0,1]")
  require(maxRecordsStatePercentage.forall(_ >= maxRecordsQueries), "Max records state percentage must be greater than max records queries")
  require(algorithms.nonEmpty, "At least one algorithm must be executed")
}

object TestDemo extends App {
  //For most of the configurations execute only ASKE
  private val alphasConfigurations = Seq(
    ExecutionConfiguration(alpha = 0.25, groupBySet = 2, maxRecordsStatePercentage = Set(0.05), maxRecordsQueries = 0.05, algorithms = Set(ASKE)),
    ExecutionConfiguration(alpha = 0.75, groupBySet = 2, maxRecordsStatePercentage = Set(0.05), maxRecordsQueries = 0.05, algorithms = Set(ASKE)))

  private val groupByConfigurations = Seq(ExecutionConfiguration(alpha = 0.5,
    groupBySet = 3,
    maxRecordsStatePercentage = Set(0.05),
    maxRecordsQueries = 0.05,
    algorithms = Set(ASKE)
  ))

  private val stateCapacityConfigurations = Set(0.01, 0.025, 0.05, 0.075, 0.1)
    .map(sc => ExecutionConfiguration(alpha = 0.5, groupBySet = 2, maxRecordsStatePercentage = Set(sc), maxRecordsQueries = sc, algorithms = Set(ASKE))).toSeq ++
    Seq(ExecutionConfiguration(alpha = 0.5, groupBySet = 2, maxRecordsStatePercentage = Set(0.05), maxRecordsQueries = 0.05, algorithms = Set(ASE, AS1, Naive)))

  FullDatasetsTestPaper(alphasConfigurations ++ groupByConfigurations ++ stateCapacityConfigurations, syntheticDatasets)

  FullDatasetsTestPaper(Seq(ExecutionConfiguration(
    alpha = 0.5,
    groupBySet = 2,
    maxRecordsStatePercentage = Set(),
    maxRecordsQueries = 1,
    algorithms = Set(Naive)
  )), numberOfWindowsToConsider = _ => 3, datasets = syntheticDatasets)

  //knapsack tests
  FullDatasetsTestPaper(Seq(ExecutionConfiguration(
    alpha = 0.5,
    groupBySet = 2,
    maxRecordsStatePercentage = Set(0.05),
    maxRecordsQueries = 0.05,
    algorithms = Set(ASKE, ASE, AS1, Naive)
  )), datasets = syntheticKnapsackDatasets, numberOfWindowsToConsider = _ => 30)

  val frequencies = Seq(1, 2, 5) ++ Seq.range(20, 70, 10) // here filter 10 in order to have no duplicates with the other tests
  frequencies.foreach(f => FullDatasetsTestPaper(Seq(ExecutionConfiguration(
    alpha = 0.5,
    groupBySet = 2,
    maxRecordsStatePercentage = Set(0.05),
    maxRecordsQueries = 0.05,
    algorithms = Set(ASKE)
  )), datasets = Seq(Synthetic("full_sim")), frequency = f))

  //fix frequency to 10
  val paneSizes = Seq(1000, 2500, 5000, 7500, 25000) // here filter 10000 in order to have no duplicates with the other tests
  paneSizes.foreach(p => {
    val numberOfPanes = 50000 / p
    FullDatasetsTestPaper(Seq(ExecutionConfiguration(
      alpha = 0.5,
      groupBySet = 2,
      maxRecordsStatePercentage = Set(0.05),
      maxRecordsQueries = 0.05,
      algorithms = Set(ASKE)
    )), numberOfPanes = numberOfPanes, numberOfRecordsPane = p, numberOfWindowsToConsider = _  => numberOfPanes * 3, datasets = Seq(Synthetic("full_sim")))
  })
}
