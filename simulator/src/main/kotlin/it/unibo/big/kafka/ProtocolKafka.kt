package it.unibo.big.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

/**
 * The Kafka protocol class
 */
class ProtocolKafka {
    private val props = Properties()
    private var producer: KafkaProducer<String, String>? = null

    /**
     * Function for register the kafka protocol
     */
    fun register(broker: String) {
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = broker
        /*props["acks"] = "all"
        props["retries"] = 10
        props["linger.ms"] = 1*/
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        producer = KafkaProducer(props)
    }

    /**
     * Function to send a message on a topic
     *
     * @param key the message key
     * @param payload the message payload
     * @param topic the kafka topic name (remember to create it for avoid Spark errors!)
     */
    fun send(key: String, payload: String, topic: String) {
        producer!!.send(ProducerRecord(topic, key, payload))
    }

    fun close() {
        producer!!.close()
    }
}