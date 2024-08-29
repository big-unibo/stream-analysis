package it.unibo.big.analysis.simulation

import it.unibo.big.analysis.configuration.Config
import it.unibo.big.kafka.ProtocolKafka
import kotlinx.coroutines.*

enum class DatasetType {
    SYNTHETIC,
    BITBANG,
    WELASER
}

/**
 * Class to generate data and send it to a Kafka topic
 */
class ServerGenerator {
    private var job: Job? = null
    private var running = false
    private val protocolKafka = ProtocolKafka()

    /**
     * Start the generation of real data
     * @param datasetType the type of dataset to generate
     * @param frequency the frequency of the generation
     * @param topic the Kafka topic to send the data to
     */
    fun start(datasetType: DatasetType, frequency: Long, topic: String, broker: String) {
        running = true
        job = CoroutineScope(Dispatchers.Default).launch {
            val realRecordsProducer = RealRecordsProducer(datasetType)
            protocolKafka.register(broker)
            try {
                simulate(null, frequency,
                    { jsonData, time -> writingFunction(datasetType, null, null, protocolKafka, topic, frequency, jsonData, time) },
                    { _, _ -> realRecordsProducer.getNext() }
                )
            } catch (e: CancellationException) {
                println("Generation stopped: ${e.message}")
            }
        }
    }

    /**
     * Start the generation of synthetic data
     * @param config the configuration of the generation
     * @param impact the impact of the data variation
     * @param extension the extension of the data variation
     * @param topic the Kafka topic to send the data to
     */
    fun start(config: Config, impact: Double, extension: Double, topic: String, broker: String) {
        running = true
        job = CoroutineScope(Dispatchers.Default).launch {
            protocolKafka.register(broker)
            try {
                simulate(config.duration, config.frequency!!,
                    { jsonData, time -> writingFunction(DatasetType.SYNTHETIC, impact, extension, protocolKafka, topic, config.frequency, jsonData, time) },
                    { elapsedTime, random -> getSyntheticRecord(config, elapsedTime, random) }
                )
            } catch (e: CancellationException) {
                println("Generation stopped: ${e.message}")
            }
        }
    }

    /**
     * Function to write data to a Kafka topic
     * @param datasetType the type of dataset to generate
     * @param impact the impact of the data variation
     * @param extension the extension of the data variation
     * @param protocolKafka the Kafka protocol to use
     * @param topic the Kafka topic to send the data to
     * @param frequency the frequency of the generation
     * @param jsonData the data to send
     * @param time the time of the data
     * @throws InterruptedException if the thread is interrupted
     */
    private fun writingFunction(
        datasetType: DatasetType,
        impact: Double?,
        extension: Double?,
        protocolKafka: ProtocolKafka,
        topic: String,
        frequency: Long,
        jsonData: Map<String, Comparable<*>?>,
        time: Long
    ) {
        val stringValue = "$time,\"${jsonData.map { (k, v) -> "$k=$v" }.joinToString(", ")}\""
        var dataset = datasetType.toString()
        if(datasetType == DatasetType.SYNTHETIC) {
            if (impact!! != 0.0 || extension!! != 0.0) {
                dataset += "_${impact}_${extension}"
            }
        }
        protocolKafka.send(dataset, stringValue, topic)
        Thread.sleep(frequency)
    }

    /**
     * Stop the generation of data
     */
    fun stop() {
        if(running) {
            running = false
            protocolKafka.close()
            job?.cancel()
        }
    }

    /**
     * Check if the generation is running
     */
    fun isRunning(): Boolean {
        return running
    }
}