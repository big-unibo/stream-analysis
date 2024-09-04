package it.unibo.big.query.execution

import it.unibo.big.input.SimulationConfiguration
import it.unibo.big.query.app.DatasetsUtils.SyntheticDataset

object QueryExecutionTimeUtils {
  import com.typesafe.config.{Config, ConfigFactory}
  private val config: Config = ConfigFactory.load("analysis_configuration.conf")

  /**
   *
   * @param configuration the simulation configuration
   * @param availableTime the available time
   * @return the execution time of the query for the given configuration
   */
  def getExecutionTime(configuration: SimulationConfiguration, queryDimensions: Int, availableTime: Long): Long = {
    val datasetName = configuration.dataset match {
      case d if d.isInstanceOf[SyntheticDataset] => "SyntheticDataset"
    }
    val datasetConfiguration = config.getConfig(s"simulation.datasets.$datasetName")
    val executionTimes = datasetConfiguration.getConfigList("executionTimes")

    executionTimes.toArray.collectFirst {
      case x if x.asInstanceOf[Config].getInt("queryDimensions") == queryDimensions
        && x.asInstanceOf[Config].getLong("slideDuration") == configuration.slideDuration => x.asInstanceOf[Config].getLong("executionTime")
    }.getOrElse(throw new IllegalArgumentException(s"Execution time not found for dataset $datasetName and query dimensions $queryDimensions"))
  }

}