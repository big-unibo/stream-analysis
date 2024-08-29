package it.unibo.big.analysis.simulation

import com.typesafe.config.ConfigFactory
import it.unibo.big.analysis.configuration.DataVariation
import it.unibo.big.analysis.configuration.loadConfig

fun main() {
    val config = ConfigFactory.parseResources("analysis_configuration.conf")

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