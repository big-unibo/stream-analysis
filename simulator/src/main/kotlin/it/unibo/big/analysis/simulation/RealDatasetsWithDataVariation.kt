package it.unibo.big.analysis.simulation

import com.typesafe.config.ConfigFactory
import it.unibo.big.analysis.configuration.DimensionConfig
import it.unibo.big.analysis.configuration.Disappear
import it.unibo.big.analysis.configuration.Fixed
import kotlin.math.max
import kotlin.math.min

/**
 * Main function for the simulation of real dataset with data variation
 */
fun main() {
    val config = ConfigFactory.parseResources("analysis_configuration.conf")
    val slideDuration = config.getLong("dataVariations.slideDuration")
    val windowDuration = config.getLong("dataVariations.windowDuration")
    val outputPath = config.getString("dataVariations.output_folder")
    //val impact = 0.8
    //val extension = 0.8
    val frequency = 1L
    val duration = config.getLong("dataVariations.duration")
    val dimensionsThreshold = 22
    val dimensionsStep = 15
    val dimensionsToConsider = listOf("location", "_id")

    /*val dataVariation = DataVariation(
        delay = windowDuration,
        frequency = slideDuration,
        extension = extension,
        impact = impact
    )*/

    val datasets = listOf(DatasetType.WELASER)
    datasets.forEach { it ->
        //save in a map with string and in for each record the distinct values
        val dimensions = emptyMap<String, Pair<Set<String>, Int>>().toMutableMap()
        var dimensionsConfig = emptyList<DimensionConfig>()
        val realRecordsProducer = RealRecordsProducer(it, sample = false)
        val realRecords: List<Map<String, Comparable<*>>> = realRecordsProducer.records()
        //for each key that is string group by key and collect the distinct values
        val dimensionsValues = realRecords
            .flatMap { it.entries }
            .filter { it.value is String }
            .groupBy({ it.key }) { it.value.toString() }
            .mapValues { (_, values) -> values.toSet().toList() }

        var dataIndex = -1
        var readedRecords = -1
        try {
            println("Start simulation for dataset: $it, duration: $duration, records = ${realRecords.size}")
            simulate(duration, frequency, "${outputPath}${it.name}_mod.csv")
            { elapsedTime, random ->
                if (dataIndex >= realRecords.size - 1) {
                    dataIndex = -1
                }
                if (elapsedTime >= windowDuration) {
                    dataIndex += 1
                    val record = realRecords[dataIndex]
                    if (dimensionsConfig.isEmpty()) {
                        dimensionsConfig = dimensions.map {
                            DimensionConfig(
                                it.key,
                                Fixed(min(1.0, it.value.second / readedRecords.toDouble()), it.value.first.size)
                            )
                        }
                    }
                    if (elapsedTime % slideDuration == 0L) {
                        //apply variation to dimensions
                        dimensionsConfig = dimensionsConfig.map {
                            if (it.name in dimensionsToConsider) {
                                //disappear for the considered dimensions, the disapper is applied for two windows
                                DimensionConfig(
                                    it.name,
                                    Disappear(
                                        max(0.1, it.behavior.getProbability(elapsedTime)),
                                        0L,
                                        slideDuration * 2,
                                        it.behavior.getValues(elapsedTime)
                                    )
                                )
                            } else {
                                /*if(it.behavior.getValues(elapsedTime) > dimensionsThreshold && it.behavior.getProbability(elapsedTime) > 0.1) {
                                    DimensionConfig(it.name, Fixed(0.2, dimensionsThreshold))
                                } else if(it.behavior.getProbability(elapsedTime) < 1.0) {
                                    DimensionConfig(it.name, IncreaseProbability(max(0.1, it.behavior.getProbability(elapsedTime)), 1.0, 0, slideDuration, min(dimensionsStep, it.behavior.getValues(elapsedTime) + dimensionsStep)))
                                } else {
                                    DimensionConfig(it.name, Fixed(min(1.0, it.behavior.getProbability(elapsedTime)), min(dimensionsThreshold, it.behavior.getValues(elapsedTime) + dimensionsStep)))
                                }*/
                                it
                            }
                        }
                    }
                    //get next record
                    val dimensionsProbabilities = dimensionsConfig.associate { d ->
                        d.name to Triple(
                            d,
                            random.nextDouble() <= d.behavior.getProbability(elapsedTime),
                            random.nextInt(d.behavior.getValues(elapsedTime))
                        )
                    }
                    val jsonData: Map<String, Comparable<*>?> = dimensionsProbabilities.mapValues { (dim, value) ->
                        if (value.second) {
                            getNextValue(dimensionsValues[dim] ?: emptyList(), value.third)
                        } else {
                            null
                        }
                    } + record.filter { (_, v) -> v !is String }
                    jsonData
                } else {
                    dataIndex += 1
                    readedRecords += 1
                    val record: Map<String, Comparable<*>> = realRecords[dataIndex]
                    val jsonData = dimensionsValues.mapValues { (key, values) ->
                        if (key in dimensionsToConsider || random.nextDouble() <= 0.9) {
                            val value = if (key in dimensionsToConsider) {
                                getNextValue(values, random.nextInt(min(values.size, dimensionsThreshold)))
                            } else if (values.size < dimensionsThreshold) {
                                getNextValue(values, random.nextInt(max(dimensionsStep, values.size)))
                            } else {
                                getNextValue(values, random.nextInt(values.size))
                            }

                            if (dimensions.containsKey(key)) {
                                val (distinctValues, count) = dimensions[key]!!
                                dimensions[key] = Pair(distinctValues + value, count + 1)
                            } else {
                                dimensions[key] = Pair(setOf(value), 1)
                            }
                            value
                        } else {
                            null
                        }
                    }.filterValues { it != null } + record.filter { (_, v) -> v !is String }
                    jsonData
                }
            }
        } catch (e: Exception) {
            e.printStackTrace()
            println("Generation stopped: ${e.message}")
        }
    }
}

private fun getNextValue(
    values: List<String>,
    index: Int
): String {
    val adjustedIndex = index % values.size
    return values[adjustedIndex] + " ${index}"
}