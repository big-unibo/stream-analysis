package it.unibo.big.query.app

import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.input.reader.ReaderUtils.readRecords
import it.unibo.big.query.state.StateUtils.StateType

/**
 * Utils for datasets definition
 */
object DatasetsUtils {

  sealed trait Dataset {
    val datasetType: String
    val datasetName: String
    def path(stateType: StateType) = s"test/$datasetType/$datasetName/${stateType.extraPath}"
    def fileName: String
    def reader: Iterator[Record]
  }

  sealed trait SyntheticDataset extends Dataset {
    override val datasetType: String = "synthetic"
    override def reader : Iterator[Record] = readRecords(fileName, readAsAStream = true)
  }

  case class Synthetic(datasetName: String) extends SyntheticDataset {
    override val fileName: String = s"test/$datasetName.csv"
  }

  case class ChangingSyntheticDataset(impact: Double, extension: Double) extends SyntheticDataset {
    override val datasetName: String = s"full_sim_impact${impact}extension$extension"
    override val fileName: String = s"test/$datasetName.csv"
  }

  private val ranges = Seq(0.2, 0.5, 0.8)
  val syntheticDatasets: Seq[SyntheticDataset] = ranges.flatMap(impact => ranges.map(extension => ChangingSyntheticDataset(impact, extension))) :+ Synthetic("full_sim")
  val syntheticKnapsackDatasets: Seq[Synthetic] = Seq(1).map(i => Synthetic(s"full_sim_knapsack_0$i"))
}
