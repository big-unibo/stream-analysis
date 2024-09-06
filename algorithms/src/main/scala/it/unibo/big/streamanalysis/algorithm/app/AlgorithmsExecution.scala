package it.unibo.big.streamanalysis.algorithm.app

import it.unibo.big.streamanalysis.utils.DatasetsUtils.{Synthetic, syntheticDatasets, syntheticKnapsackDatasets}
import ExecutionUtils.{AS1, ASE, ASKE, Naive}
import ExecutionUtils.{AS1, ASE, ASKE, Naive}

object AlgorithmsExecution extends App {
  //For most of the configurations execute only ASKE
  private val alphasConfigurations = Seq(
    ExecutionConfiguration(alpha = 0.25, k = 2, stateCapacities = Set(0.05), algorithms = Set(ASKE)),
    ExecutionConfiguration(alpha = 0.75, k = 2, stateCapacities = Set(0.05), algorithms = Set(ASKE)))

  private val groupByConfigurations = Seq(ExecutionConfiguration(alpha = 0.5,
    k = 3,
    stateCapacities = Set(0.05),
    algorithms = Set(ASKE)
  ))

  private val stateCapacityConfigurations = Set(0.01, 0.025, 0.05, 0.075, 0.1)
    .map(sc => ExecutionConfiguration(alpha = 0.5, k = 2, stateCapacities = Set(sc), algorithms = Set(ASKE))).toSeq ++
    Seq(ExecutionConfiguration(alpha = 0.5, k = 2, stateCapacities = Set(0.05), algorithms = Set(ASE, AS1, Naive)))

  ExecutionUtils(alphasConfigurations ++ groupByConfigurations ++ stateCapacityConfigurations, syntheticDatasets)

  ExecutionUtils(Seq(ExecutionConfiguration(
    alpha = 0.5,
    k = 2,
    maximumQueryCardinality = 1,
    algorithms = Set(Naive)
  )), numberOfWindowsToConsider = _ => 3, datasets = syntheticDatasets)

  private val frequencies = Seq(1, 2, 5) ++ Seq.range(20, 70, 10) // here filter 10 in order to have no duplicates with the other tests
  frequencies.foreach(f => ExecutionUtils(Seq(ExecutionConfiguration(
    alpha = 0.5,
    k = 2,
    stateCapacities = Set(0.05),
    algorithms = Set(ASKE)
  )), datasets = Seq(Synthetic("full_sim")), frequency = f))

  //fix frequency to 10
  private val paneSizes = Seq(1000, 2500, 5000, 7500, 25000) // here filter 10000 in order to have no duplicates with the other tests
  paneSizes.foreach(p => {
    val numberOfPanes = 50000 / p
    ExecutionUtils(Seq(ExecutionConfiguration(
      alpha = 0.5,
      k = 2,
      stateCapacities = Set(0.05),
      algorithms = Set(ASKE)
    )), numberOfPanes = numberOfPanes, numberOfRecordsPane = p, numberOfWindowsToConsider = _  => numberOfPanes * 3, datasets = Seq(Synthetic("full_sim")))
  })
}