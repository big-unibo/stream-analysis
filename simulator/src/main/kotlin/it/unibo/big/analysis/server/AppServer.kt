package it.unibo.big.analysis.server

import com.typesafe.config.ConfigFactory
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import it.unibo.big.analysis.configuration.DataVariation
import it.unibo.big.analysis.configuration.loadConfig
import it.unibo.big.analysis.simulation.DatasetType
import it.unibo.big.analysis.simulation.ServerGenerator
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
data class Response(val status: Int, val message: String)

/**
 * Main function to start the server
 */
fun main() {
    val config = ConfigFactory.parseResources("analysis_configuration.conf")
    val simulationConfiguration = loadConfig(config.getString("dataVariations.input"))
    val defaultChannel = config.getString("kafka.channel")
    val broker = config.getString("kafka.broker")
    val port = config.getInt("server.port")
    val generator = ServerGenerator()

    embeddedServer(Netty, port = port) {
        install(ContentNegotiation) {
            json(Json {
                prettyPrint = true
                isLenient = true
            })
        }
        routing {
            get("/generator") {
                if(generator.isRunning()) {
                    call.respond(Response(HttpStatusCode.BadRequest.value, "Generation is already running."))
                }
                val datasetTypeParam  = call.request.queryParameters["type"]
                val frequency = call.request.queryParameters["frequency"]?.toLongOrNull()
                val impact = call.request.queryParameters["impact"]?.toDoubleOrNull()
                val extension = call.request.queryParameters["extension"]?.toDoubleOrNull()
                val changeDuration = call.request.queryParameters["changeDuration"]?.toLongOrNull()

                if (datasetTypeParam.isNullOrEmpty() || frequency == null) {
                    call.respond(Response(HttpStatusCode.BadRequest.value, "Missing required parameters: type, frequency."))
                    return@get
                }
                // Parse dataset type from string to enum
                val datasetType = try {
                    DatasetType.valueOf(datasetTypeParam.toUpperCase())
                } catch (e: IllegalArgumentException) {
                    call.respond(Response(HttpStatusCode.BadRequest.value, "Invalid type parameter."))
                    return@get
                }
                // Process datasetType accordingly
                when (datasetType) {
                    DatasetType.SYNTHETIC -> {
                        if (impact == null || extension == null || changeDuration == null) {
                            call.respond(Response(HttpStatusCode.BadRequest.value, "Missing required parameters for synthetic dataset: impact, extension and changeDuration."))
                            return@get
                        }

                        if (impact !in 0.0..1.0 || extension !in 0.0..1.0) {
                            call.respond(Response(HttpStatusCode.BadRequest.value, "Impact and extension must be in the range 0-1."))
                            return@get
                        }
                        val conf = simulationConfiguration.copy(
                            dataVariation = DataVariation(
                                delay = changeDuration,
                                frequency = changeDuration,
                                extension = extension,
                                impact = impact
                            ),
                            duration = null,
                            frequency = frequency
                        )
                        //respond as json message
                        call.respond(Response(HttpStatusCode.OK.value, "Generating synthetic dataset with frequency $frequency, impact $impact, and extension $extension."))
                        generator.start(conf, impact, extension, defaultChannel, broker)
                    }
                    else -> {
                        call.respond(Response(HttpStatusCode.OK.value, "Generating $datasetType dataset with frequency $frequency."))
                        generator.start(datasetType, frequency, defaultChannel, broker)
                    }
                }
            }
            get("/stop") {
                if(generator.isRunning()) {
                    generator.stop()
                    call.respond(Response(HttpStatusCode.OK.value, "Generation stopped."))
                } else {
                    call.respond(Response(HttpStatusCode.BadRequest.value, "Generation is not running."))
                }
            }
        }
    }.start(wait = true)
}