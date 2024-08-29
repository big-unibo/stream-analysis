package it.unibo.big.query.app

import com.typesafe.config.{Config, ConfigFactory}
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import io.vertx.core._
import io.vertx.ext.web.{Router, RoutingContext}
import it.unibo.big.kafka.KafkaConfiguration
import org.slf4j.{Logger, LoggerFactory}

class QueryAnalysisVerticle extends AbstractVerticle {
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  private val serviceConfig: Config = ConfigFactory.load("analysis_configuration.conf").getConfig ( "server" )
  private val kafkaConfiguration = KafkaConfiguration(serviceConfig)
  private var simulationStarted = false

  private var router : Router = _

  override def init ( vertx: Vertx, context: Context ): Unit = {
    super.init ( vertx, context )
    router = Router.router ( vertx )
  }

  /** Start the verticle */
  override def start ( ): Unit = {
    router.get ( "/algorithm" ).handler ( new Handler[RoutingContext] {
      override def handle ( req: RoutingContext ): Unit =
        try {
          val params = req.queryParams()
          val windowDuration = params.get("windowDuration").toLong
          val slideDuration = params.get("slideDuration").toLong
          val k = params.get("k").toInt
          val alpha = params.get("alpha").toDouble
          val stateCapacity = params.get("stateCapacity").toDouble
          if (windowDuration <= 0) {
            sendError("windowDuration must be > 0", req.response())
          } else if (slideDuration <= 0) {
            sendError("slideDuration must be > 0", req.response())
          } else if (k <= 0) {
            sendError("k must be > 0", req.response())
          } else if (alpha < 0 || alpha > 1) {
            sendError("alpha must be in range 0...1", req.response())
          } else if (stateCapacity <= 0 || stateCapacity > 1) {
            sendError("stateCapacity must be in range ]0,1]", req.response())
          } else {
            if(!simulationStarted) {
              simulationStarted = true
              vertx.executeBlocking(new Handler[Promise[Void]] {
                override def handle(promise: Promise[Void]): Unit = {
                  // Implement your blocking logic here
                  SingleAlgorithmExecution.executeSimulation(windowDuration, slideDuration, k, alpha, stateCapacity, kafkaConfiguration)
                  promise.complete()
                }
              }, new Handler[AsyncResult[Void]] {
                override def handle(res: AsyncResult[Void]): Unit = {
                  if (res.succeeded()) {
                    LOGGER.info("Simulation completed")
                  } else {
                    LOGGER.info("Simulation stopped")
                  }
                }
              })

              req.response
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("status", 200).put("message", "Algorithm computation started").encodePrettily())
            } else {
              sendError("Simulation already started", req.response())
            }
          }
        } catch {
          case e: Throwable =>
            simulationStarted = false
            SingleAlgorithmExecution.stopSimulation()
            e.printStackTrace()
            sendError(e.getMessage, req.response())
        }
    })
    router.get("/stop").handler(new Handler[RoutingContext] {
      override def handle(req: RoutingContext): Unit = {
        if (simulationStarted) {
          try {
            simulationStarted = false
            // Implement your stop logic here
            SingleAlgorithmExecution.stopSimulation()
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            req.response()
              .putHeader("content-type", "application/json")
              .end(new JsonObject().put("status", 200).put("message", "Simulation stopped").encodePrettily())
          }
        } else {
          sendError("No simulation is running", req.response())
        }
      }
    })

    LOGGER.info (s"Started on port ${serviceConfig.getInt ( "port" )}")

    vertx
      .createHttpServer ()
      .requestHandler ( router )
      .listen ( serviceConfig.getInt ( "port" ), serviceConfig.getString ( "host" ))
  }

  private def sendError(errorMessage: String, response: HttpServerResponse, statusCode: Int = 400): Unit = {
    response.setStatusCode(statusCode)
      .putHeader("content-type", "application/json")
      .end(new JsonObject().put("status", 400).put("message", errorMessage).encodePrettily())
  }
}

object LaunchServer extends App {
  Vertx.vertx.deployVerticle (new QueryAnalysisVerticle)
}
