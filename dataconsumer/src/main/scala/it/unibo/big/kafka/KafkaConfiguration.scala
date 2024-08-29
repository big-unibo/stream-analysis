package it.unibo.big.kafka

import com.typesafe.config.Config

/**
 * Configuration for Kafka
 *
 * @param inputTopic input topic
 * @param outputTopic output topic
 * @param broker broker
 */
case class KafkaConfiguration(inputTopic: String, outputTopic: String, broker: String)

object KafkaConfiguration {

  /**
   * Create a KafkaConfiguration from a Config
   * @param config the configuration
   * @return the KafkaConfiguration
   */
  def apply(config: Config): KafkaConfiguration = {
    val inputTopic = config.getString("kafka.inputTopic")
    val outputTopic = config.getString("kafka.outputTopic")
    val broker = config.getString("kafka.broker")
    new KafkaConfiguration(inputTopic, outputTopic, broker)
  }
}