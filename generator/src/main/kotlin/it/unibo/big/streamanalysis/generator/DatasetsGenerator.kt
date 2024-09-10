package it.unibo.big.streamanalysis.generator

import com.typesafe.config.ConfigFactory
import it.unibo.big.streamanalysis.generator.configuration.DataVariation
import it.unibo.big.streamanalysis.generator.configuration.loadConfig

fun main() {
    val config = ConfigFactory.parseResources("analysis_configuration.conf")
    // Get the output directory from the configuration
    config.getList("analysis").forEach {
        val values = it.unwrapped() as Map<*, *>
        val simulationConfig = loadConfig(values["input"].toString())
        simulate(
            simulationConfig.duration,
            simulationConfig.frequency!!,
            values["output"].toString()
        ) { elapsedTime, random ->
            getSyntheticRecord(
                simulationConfig,
                elapsedTime,
                random
            )
        }
    }

    //generate of scenarios datasets
    val ranges = config.getDoubleList("dataVariations.ranges")
    val slideDuration = config.getLong("dataVariations.slideDuration")
    val windowDuration = config.getLong("dataVariations.windowDuration")
    val simulationConfiguration = loadConfig(config.getString("dataVariations.input"))
    val outputPath = config.getString("dataVariations.output")
    val duration = config.getLong("dataVariations.duration")

    ranges.forEach { extension ->
        ranges.forEach { impact ->
            val configuration = simulationConfiguration.copy(
                dataVariation = DataVariation(
                    delay = windowDuration,
                    frequency = slideDuration,
                    extension = extension,
                    impact = impact
                ),
                duration = duration
            )
            simulate(duration, configuration.frequency!!, "${outputPath}impact${impact}extension${extension}.csv")
            { elapsedTime, random -> getSyntheticRecord(configuration, elapsedTime, random) }
        }
    }
}