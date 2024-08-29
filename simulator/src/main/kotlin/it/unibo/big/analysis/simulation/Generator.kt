package it.unibo.big.analysis.simulation

import com.typesafe.config.ConfigFactory
import it.unibo.big.analysis.configuration.loadConfig

fun main() {
    val config = ConfigFactory.parseResources("analysis_configuration.conf")
    // Get the output directory from the configuration
    val simulationConfig = loadConfig(config.getString("analysis.input"))
    simulate(simulationConfig.duration, simulationConfig.frequency!!, config.getString("analysis.output")) { elapsedTime, random ->
        getSyntheticRecord(
            simulationConfig,
            elapsedTime,
            random
        )
    }
}