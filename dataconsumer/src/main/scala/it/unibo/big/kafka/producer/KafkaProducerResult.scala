package it.unibo.big.kafka.producer

import it.unibo.big.kafka.KafkaConfiguration
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import java.util.concurrent.Future

/**
 * A scala-kafka consumer
 *
 * @param kafkaConfiguration the kafka configuration
 */
class KafkaProducerResult(kafkaConfiguration: KafkaConfiguration) {
  private val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.broker)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  private val producer = new KafkaProducer[String, String](props)

  /**
   * Send a message to the output topic
   * @param resultKeys the keys of the result
   * @param resultRecords the records of the result
   * @param dataDimensions the dimensions support
   * @param size the size of the pane
   * @param paneTime the time of the pane
   * @return the future of the record metadata
   */
  def sendMessage(resultKeys: Seq[String], resultRecords: Seq[Seq[Any]], dataDimensions: Map[String, Double], size: Int, paneTime: Long): Future[RecordMetadata] = {
    //create json to send data
    val resultString =
      s"""{"result": "${resultKeys.mkString(",") + "\n" + resultRecords.map(_.mkString(",")).mkString("\n")}",
         |"dimensions-support": ${dataDimensions.map(x => s"""\"${x._1}\": ${x._2}""").mkString("{", ",", "}")},
         |"paneSize": $size, "paneTime": $paneTime}""".stripMargin

    val record = new ProducerRecord[String, String](kafkaConfiguration.outputTopic, resultString)
    producer.send(record)
  }

  /**
   * Close the producer
   */
  def close() = {
    producer.close()
  }
}
