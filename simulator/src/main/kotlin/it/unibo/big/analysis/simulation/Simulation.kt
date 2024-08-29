package it.unibo.big.analysis.simulation

import it.unibo.big.analysis.configuration.Config
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.io.File
import java.io.FileOutputStream
import kotlin.random.Random

/**
 * Main function for the simulation
 * @param duration the duration of the simulation
 * @param frequency the frequency of the simulation
 * @param outputPath the output path of the csv file
 * @param getRecord the function to get the record
 */
fun simulate(duration: Long?, frequency: Long, outputPath: String, getRecord: (Long, java.util.Random) -> Map<String, Comparable<*>?>) {
    val csvFile = File(outputPath)
    val outputDirectory = csvFile.parent
    // Create the output directory if it doesn't exist
    val outputDirectoryFile = File(outputDirectory)
    if (!outputDirectoryFile.exists()) {
        outputDirectoryFile.mkdirs()
    }

    val csvPrinter = CSVPrinter(FileOutputStream(csvFile).bufferedWriter(), CSVFormat.DEFAULT)
    csvPrinter.printRecord(listOf("time", "value"))

    simulate(duration, frequency, { jsonData, time ->
        csvPrinter.printRecord(listOf(time, jsonData.map{ (k, v) -> "$k=$v" }.joinToString(", ")))
    }, getRecord)

    csvPrinter.flush()
    csvPrinter.close()
}

/**
 * Function for simulate the data generation
 * @param duration the duration of the simulation
 * @param frequency the frequency of the simulation
 * @param writeAction the action to write the data to an output
 * @param getRecord the function to get the record
 */
fun simulate(duration: Long?, frequency: Long, writeAction: (Map<String, Comparable<*>?>, Long) -> Unit, getRecord: (Long, java.util.Random) -> Map<String, Comparable<*>?>) {
    val random = java.util.Random()
    val startTime = System.currentTimeMillis()
    var time = startTime

    while (if(duration != null) time - startTime < duration else true) {
        val elapsedTime = time - startTime
        val jsonData: Map<String, Comparable<*>?> = getRecord(elapsedTime, random)


        writeAction(jsonData, time)
        time += frequency
    }
    println("Simulation completed, elapsed time ${time - startTime} duration $duration")
}

/**
 * Function to get a synthetic record
 * @param config the configuration of the generation
 * @param elapsedTime the elapsed time
 * @param random the random generator
 * @return the synthetic record
 */
fun getSyntheticRecord(
    config: Config,
    elapsedTime: Long,
    random: java.util.Random
): Map<String, Comparable<*>?> {
    //get the behaviour of each dimension
    config.applyDataVariation(elapsedTime)

    //generate a map where for each dimension the number of values is the one specified in the configuration
    val dimensionNumberOfValues = config.getDimensions().associate { it.name to it.behavior.getValues(elapsedTime) }

    //generate values for all the dimensions from 1 to values like 1-dimensionName
    val dimensionsValues = config.getDimensions().associate {
        it.name to (1..dimensionNumberOfValues[it.name]!!).map { i -> i to "${it.name}-${i}" }
    }

    val dimensionsProbabilities = config.getDimensions().map { dimensionConfig ->
        dimensionConfig.name to Triple(
            dimensionConfig,
            Random.nextDouble() <= dimensionConfig.behavior.getProbability(elapsedTime),
            Random.nextInt(dimensionNumberOfValues[dimensionConfig.name]!!)
        )
    }.toMap()

    val measuresProbabilities = config.measures.map { measureConfig ->
        measureConfig to (Random.nextDouble() <= measureConfig.behavior.getProbability(elapsedTime))
    }.toMap()

    // Generate the JSON data starting from the probabilities
    val jsonData: Map<String, Comparable<*>?> = dimensionsProbabilities.mapValues { (dim, value) ->
        if (value.second) {
            //if the dimension is in a hierarchy and is not the first element of the hierarchy, get the value from the hierarchy
            val hierarchy = config.hierarchies.find { it.contains(dim) && it.indexOf(dim) != 0 }
            val index = hierarchy?.indexOf(dim) ?: -1
            val filteredHierarchy = hierarchy?.take(index + 1)
            val v = filteredHierarchy?.joinToString("-") {
                dimensionsValues[it]?.get(dimensionsProbabilities[it]!!.third)?.second
                    ?: throw IllegalArgumentException("Dimension $it not found")
            }
                ?: dimensionsValues[dim]?.get(value.third)?.second
            v
        } else {
            null
        }
    } + measuresProbabilities.map { (measure, generate) ->
        measure.name to (if (generate) random.nextGaussian() * measure.stdDev + measure.mean else null)
    }
    return jsonData
}