package it.unibo.big.analysis.configuration

import com.charleskorn.kaml.Yaml
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import java.util.stream.Collectors

/**
 *  Configuration of the simulation
 *  @param dimensions the dimensions to generate
 *  @param measures the measures to generate
 *  @param frequency the frequency of the generation
 *  @param hierarchies the hierarchies of the attributes
 *  @param duration the duration of the simulation
 *  @param dataVariation the optional data variation option
 */
@Serializable
data class Config(
    private var dimensions: List<DimensionConfig> = emptyList(),
    val measures: List<MeasureConfig> = emptyList(),
    val frequency: Long? = null,
    val hierarchies: List<List<String>> = emptyList(),
    val duration: Long? = null,
    private var dataVariation: DataVariation? = null
) {
    /**
     * Function for check if the frequency of the data variation is multiple of the frequency of the data
     */
    private fun checkDataVariation() {
        require((dataVariation!!.frequency % (frequency ?: 0L)) == 0L) { // Changed to ensure we don't divide by zero
            "dataVariation frequency must be a multiple of frequency"
        }
    }
    init {
        if(dataVariation != null) { checkDataVariation() }
    }

    fun getDimensions(): List<DimensionConfig> = dimensions

    /**
     * Apply data variation at given elapsed time
     */
    fun applyDataVariation(elapsedTime: Long) {
        if(dataVariation != null) {
            dimensions = dataVariation!!.getUpdatedBehaviours(elapsedTime, dimensions)
        }
    }

    /**
     * Change the data variation in the configuration
     * @param frequency the frequency of the variation
     * @param extension the percentage of dimensions (random) involved in a (random) data variation behavior
     * @param impact the strength of the data variation behavior
     */
    fun changeDataVariation(delay: Long, frequency: Long, extension: Double, impact: Double) {
        dataVariation = DataVariation(delay, frequency, extension, impact)
        checkDataVariation()
    }
}

/**
 * Configuration of a dimension
 * @param name the name of the dimension
 * @param behavior the behavior of the dimension
 */
@Serializable
data class DimensionConfig(
    val name: String,
    val behavior: Behavior
)

/**
 * Configuration of a measure
 * @param mean the mean of the measure
 * @param stdDev the standard deviation of the measure
 * @param behavior the behavior of the measure
 * @param name the name of the measure
 */
@Serializable
data class MeasureConfig(
    val name: String,
    val mean: Double,
    val stdDev: Double,
    val behavior: Behavior
) {
    init {
        require(stdDev >= 0) { "Standard deviation must be positive" }
        require(behavior is Fixed) { "Behavior must be fixed for measures" }
    }
}

private val yaml = Yaml(serializersModule = SerializersModule {
    polymorphic(Behavior::class) {
        subclass(Disappear::class)
        subclass(Appear::class)
        subclass(IncreaseProbability::class)
        subclass(DecreaseProbability::class)
        subclass(Fixed::class)
        subclass(IncreaseValues::class)
        subclass(DecreaseBehavior::class)
    }
})

/**
 * Load the configuration from a file
 * @param fileName the name of the file
 * @return the configuration, requiring that all the dimensions in the hierarchies are present in the dimensions
 */
fun loadConfig(fileName: String): Config {
    val inputString = object {}.javaClass.getResourceAsStream(fileName)?.bufferedReader()?.lines()?.collect(
        Collectors.joining("\n")
    ).toString()
    val config = yaml.decodeFromString(Config.serializer(), inputString)
    //require that all the dimensions in the hierarchies are present in the dimensions
    config.hierarchies.forEach { hierarchy ->
        hierarchy.forEach { dim ->
            require(config.getDimensions().any { it.name == dim }) { "Dimension $dim not found" }
        }
    }
    return config
}