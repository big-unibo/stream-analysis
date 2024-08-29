package it.unibo.big.input.reader

import com.google.common.base.Splitter
import it.unibo.big.input.DataDefinition
import it.unibo.big.input.DataDefinition._
import it.unibo.big.input.RecordModeling.Record
import it.unibo.big.query.app.DatasetsUtils.{AgriRobot, BitBang, BitBang1, BitBang2, DailyMovments, RealDataset, Technogym}
import it.unibo.big.utils.FileReader
import it.unibo.big.utils.FileReader.MyConverter
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import java.util.regex.Pattern
import scala.collection.JavaConverters._

object RealDataReader {

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  /**
   *
   * @param dataset the real dataset
   * @param sort if the data should be sorted
   * @return the iterator of the file with data
   */
  def readRealData(dataset: RealDataset, sort: Boolean): Iterator[Record] = {
    val reader = realDataReader(dataset)
    if(sort) FileReader.readFile(dataset.fileName, isResource = false)(reader).sortBy(x => (x.timestamp.getTime, x.value)).toIterator else
      FileReader.readLazyFile(dataset.fileName, isResource = false)(reader)
  }

  /**
   * Converter for the simulation with data input with Record
   */
  def realDataReader(dataset: RealDataset): MyConverter[Record] = new MyConverter[Record] {
    private val splitRegex = """,(?=([^"]*"[^"]*")*[^"]*$)"""
    private var timestamp = System.currentTimeMillis()
    private val pattern = Pattern.compile(splitRegex)
    private val splitter = Splitter.on(pattern).omitEmptyStrings().trimResults()

    private var header : Seq[String] = Seq()

    override def parse(line: String): Record = {
      val time = System.currentTimeMillis()
      val data = splitter.splitToList(line).asScala
      if(System.currentTimeMillis() - time > 100) {
        LOGGER.info(s"Split takes ${System.currentTimeMillis() - time} ms")
        LOGGER.info(line)
      }
      var dataValues: Map[String, Data[_]] = header.indices.map(i =>
        header(i) -> util.Try(DataDefinition.parse(data(i))).getOrElse(NullData)
      ).toMap
      dataValues = dataValues
        .filter(x => {
          //Filter out arrays keys, null data and arrays data
          !x._1.endsWith("[*]") && x._2.value != NullData.value && (x._2 match {
            case x: StringData if x.value.startsWith("[") && x.value.endsWith("]") => false
            case _ => true
          })
        })
      dataset match {
        case _: Technogym =>
          dataValues = dataValues
          /* consider to filter measures
          ManualLifeStyleDuration
          LowerWeight
          UpperWeight
          ManualPlayTime
          ManualTrainingCalories
          ManualTrainingDuration
          Weight
          ManualFreeTime
          ManualLifeStyleMove
          ManualRunTime
          ManualTrainingDistance
          PartitionDate
          CoreWeight
          ManualLifeStyleCalories
          ManualTrainingMove
          CyclingDistance
          RunningDistance
          These are the distinct variables
           */
        case AgriRobot =>
          dataValues = dataValues.filterKeys(x => !x.contains("serviceProvided"))
            .filterKeys(x => !Seq("warnings", "cmdList", "refRobotModel", "infos", "domain",
              "errors", "id", "category").contains(x))

        case BitBang1 =>
          dataValues = dataValues.filterKeys(x => !x.contains("jsonpayload_v1") && !x.contains("httpRequest"))
            .filterKeys(x => !Seq(
              "resource.labels.backend_service_name",
              "resource.labels.backend_service_name",
              "resource.labels.target_proxy_name",
              "resource.labels.zone",
              "resource.labels.url_map_name",
              "resource.labels.forwarding_rule_name",
              "insertId",
              "severity",
              "receiveTimestamp"
            ).contains(x))
        case BitBang2 =>
          dataValues = dataValues.filterKeys(x => !x.contains("jsonpayload_v1") && !x.contains("httpRequest"))
            .filterKeys(x => !Seq(
              "resource.labels.backend_service_name",
              "resource.labels.backend_service_name",
              "resource.labels.target_proxy_name",
              "resource.labels.zone",
              "resource.labels.url_map_name",
              "resource.labels.forwarding_rule_name"
            ).contains(x))
        case _ => //do nothing
          LOGGER.debug(s"not change this dataset $dataset")
      }

      if(dataset == DailyMovments) {
        //force measures in daily movement dataset
        val measuresToBeDimensions = Seq("WeightLifted", "ManualLifeStyleDuration", "CoreWeightLifted", "LowerWeightLifted")
        dataValues = dataValues.map {
          case (x, data) if measuresToBeDimensions.contains(x) =>
            x -> StringData(data.value.toString).asInstanceOf[Data[_]]
          case (x, data) => x -> data
        }.toMap
      }
      val foundTime = false
      //code for get the timekey from data
      /*if (dataValues.contains(dataset.timeKey)) {
        dataValues(dataset.timeKey) match {
          case n: NumericData =>
            foundTime = true
            timestamp = n.value.longValue()
          case _ =>
        }
      }*/

      //filter out the time key and the keys that are not needed
      val keysToNotConsider = (if(dataset.isInstanceOf[BitBang]) Seq("File Name", "Keys Identifier", "Keys String", "Attributes") else Seq()) :+ dataset.timeKey
      dataValues = dataValues.filterKeys(x => !keysToNotConsider.contains(x))

      val dataRow = Record(dataValues, new Timestamp(timestamp), dataset.toString)
      if(!foundTime) {
        timestamp += 1 //increment the timestamp
      }
      dataRow
    }

    override def header(headerString: String): Unit = {
      //Set the header
      header = splitter.splitToList(headerString).asScala
    }
  }
}
