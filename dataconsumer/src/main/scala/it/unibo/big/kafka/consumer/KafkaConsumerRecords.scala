package it.unibo.big.kafka.consumer

import it.unibo.big.kafka.KafkaConfiguration
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.{Collections, Properties}

/**
 * A scala-kafka consumer
 */
object KafkaConsumerRecords {
  private val OFFSET_RESET_EARLIER = "earliest"
  private val MAX_POLL_RECORDS = 1

  /**
   *
   * @param kafkaConfiguration the kafka configuration
   * @return the consumer of a pair of strings that are the message key and payload
   */
  def createConsumer(kafkaConfiguration: KafkaConfiguration): Consumer[String, String] = {
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.broker)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "stream-analysis-consumer-group")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    //props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER)
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(kafkaConfiguration.inputTopic))
    consumer
  }
}
