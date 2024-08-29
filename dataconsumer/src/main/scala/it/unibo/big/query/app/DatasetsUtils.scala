package it.unibo.big.query.app

import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.input.reader.ReaderUtils.readRecords
import it.unibo.big.input.reader.RealDataReader.readRealData
import it.unibo.big.query.state.StateUtils.StateType

/**
 * Utils for datasets definition
 */
object DatasetsUtils {

  def initializeDatasets(): Unit = {

  }
  sealed trait Dataset {
    val datasetType: String
    val datasetName: String
    def path(stateType: StateType) = s"debug/analysis/$datasetType/$datasetName/${stateType.extraPath}"
    def fileName: String
    def reader: Iterator[Record]
  }
  /*private val config: Config = ConfigFactory.load("analysis_configuration.conf")
  private val realDatasetsList = config.getList("real_data")
  private val datasetsMap = (0 until realDatasetsList.size).map(i => {
    val d = realDatasetsList.get(i).unwrapped().asInstanceOf[util.HashMap[String, String]]
    val datasetName = d.get("name")
    val fileName = d.get("file")
    datasetName -> fileName
  }).toMap*/

  sealed trait RealDataset extends Dataset {
    def timeKey: String
    override def reader : Iterator[Record] = readRealData(this, sort = false)
    override lazy val fileName: String = s"debug/analysis/$datasetName.csv"
  }

  sealed trait SyntheticDataset extends Dataset {
    override val datasetType: String = "synthetic"
    override def reader : Iterator[Record] = readRecords(fileName, readAsAStream = true)
  }

  case class Synthetic(datasetName: String) extends SyntheticDataset {
    override val fileName: String = s"debug/analysis/$datasetName.csv"
  }

  case class ChangingSyntheticDataset(impact: Double, extension: Double) extends SyntheticDataset {
    override val datasetName: String = s"full_sim_impact${impact}extension$extension"
    override val fileName: String = s"debug/analysis/$datasetName.csv"
  }

  sealed trait BitBang extends RealDataset {
    override val datasetType: String = "bitbang"
    override val timeKey: String = "Timestamp"
  }

  case object BitBang1 extends BitBang {
    override val datasetName: String = "D_real_1"
  }
  case object BitBang2 extends BitBang {
    override val datasetName: String = "D_real_2"
  }

  sealed trait WeLaser extends RealDataset {
    override val datasetType: String = "welaser"
    override val timeKey: String = "timestamp_subscription"
  }

  case object AgriRobot extends WeLaser {
    override val datasetName: String = "WELASER_mod" // "AgriRobot"
    override def reader : Iterator[Record] = readRecords(this.fileName, readAsAStream = true)
  }

  private val ranges = Seq(0.2, 0.5, 0.8)
  val syntheticDatasets: Seq[SyntheticDataset] = ranges.flatMap(impact => ranges.map(extension => ChangingSyntheticDataset(impact, extension))) :+ Synthetic("full_sim")
  val syntheticKnapsackDatasets: Seq[Synthetic] = Seq(1).map(i => Synthetic(s"full_sim_knapsack_0$i"))
  val realDatasets : Seq[RealDataset] = Seq(BitBang1, BitBang2, AgriRobot)
}
