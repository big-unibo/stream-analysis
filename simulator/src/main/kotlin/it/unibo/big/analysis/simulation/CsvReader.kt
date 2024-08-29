package it.unibo.big.analysis.simulation

import java.io.BufferedReader
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader

class RealRecordsProducer(datasetType: DatasetType, sample: Boolean = true) {
    private var index = 0
    private val regex = """,(?=([^"]*"[^"]*")*[^"]*${'$'})""".toRegex()
    private val data = when(datasetType) {
        DatasetType.BITBANG ->
            if(sample) {
                val inputStream1: InputStream = javaClass.classLoader.getResourceAsStream("bitbang_v1.csv")
                val inputStream2: InputStream = javaClass.classLoader.getResourceAsStream("bitbang_v2.csv")
                read(datasetType, 1, inputStream1) + (read(datasetType, 2, inputStream2))
            } else {
                val file1 = File("debug/analysis/D_real_1.csv").inputStream()
                val file2 = File("debug/analysis/D_real_2.csv").inputStream()
                read(datasetType, 1, file1) + read(datasetType, 2, file2)
            }
        DatasetType.WELASER ->
            if(sample) {
                read(datasetType, 1, javaClass.classLoader.getResourceAsStream("welaser_v1.csv"))
            } else {
                read(datasetType, 1, File("debug/analysis/AgriRobot.csv").inputStream())
            }
        else -> throw IllegalArgumentException("Dataset type not supported")
    }

    /**
     * Parses a line of CSV text, handling quoted fields.
     * Uses regex to split by commas while considering quoted sections.
     *
     * @param line the line of CSV text to parse
     */
    private fun parseCSVLine(line: String): List<String> {
        //split line using regex
        return line.split(regex).map { it.trim().removeSurrounding("\"") }
    }

    /**
     * Reads a CSV file and returns a list of records.
     * @param datasetType the type of dataset to read
     * @param version the version of the dataset
     * @param inputStream the input stream of the CSV file
     * @param size the number of records to read
     */
    private fun read(datasetType: DatasetType, version: Int, inputStream: InputStream, size: Int = 10000): List<Map<String, Comparable<*>>> {
        inputStream?.let { it ->
            BufferedReader(InputStreamReader(it)).use { reader ->
                val headers = reader.readLine()?.let { l -> parseCSVLine(l) } ?: return emptyList()
                val excludedColumns = getExcludedColumns(datasetType, version)

                return reader.lineSequence().take(size).map { line ->
                    val columns = parseCSVLine(line)
                    headers.indices.asSequence().mapNotNull { i ->
                        val column = columns.getOrNull(i) ?: return@mapNotNull null
                        if (!headers[i].endsWith("[*]") &&
                            column.isNotEmpty() &&
                            column != "null" &&
                            !(column.startsWith("[") && column.endsWith("]")) &&
                            !excludedColumns.contains(headers[i])
                        ) {
                            headers[i] to convertToNumberOrString(column.replace(",", "_").replace("=", ":"))
                        } else {
                            null
                        }
                    }.toMap()
                }.toList()
            }
        }
        return emptyList()
    }

    /**
     * Converts a string to a number if possible, otherwise returns the string.
     */
    private fun convertToNumberOrString(input: String): Comparable<*> =
        input.toIntOrNull() ?: input.toDoubleOrNull() ?: input

    /**
     * Returns the columns to exclude from the dataset.
     * @param datasetType the type of dataset
     * @param version the version of the dataset (for bitbang)
     */
    private fun getExcludedColumns(
        datasetType: DatasetType,
        version: Int
    ) = when (datasetType) {
        DatasetType.BITBANG ->
            setOf("jsonpayload_v1", "httpRequest", "resource.labels.backend_service_name", "resource.labels.target_proxy_name",
                "resource.labels.target_proxy_name",
                "resource.labels.zone",
                "resource.labels.url_map_name", "File Name",
                "Keys Identifier",
                "Keys String",
                "Attributes",
                "Timestamp", "resource.labels.forwarding_rule_name") + (if (version == 1) setOf("insertId", "severity", "receiveTimestamp") else emptySet())

        DatasetType.WELASER -> setOf(
            "warnings",
            "cmdList",
            "refRobotModel",
            "infos",
            "domain",
            "errors",
            "id",
            "category",
            "timestamp_subscription"
        )

        DatasetType.SYNTHETIC -> throw IllegalArgumentException("Dataset type not supported")
    }

    /**
     * Returns the records in the dataset.
     */
    fun records () : List<Map<String, Comparable<*>>>{ return data }

    /**
     * Returns the next record from the dataset.
     * @return the next record, or the first record if the end of the dataset is reached
     */
    fun getNext(): Map<String, Comparable<*>> {
        val record = data[index]
        index += 1
        if(index > data.size - 1) {
            index = 0
        }
        return record
    }
}