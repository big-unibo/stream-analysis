package it.unibo.big.streamanalysis.generator.configuration

import it.unibo.big.streamanalysis.generator.configuration.Behavior.Companion.calculateValue
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.math.round

/**
 * Interface for the behavior of an attribute
 */
@Serializable
sealed interface Behavior {
    /**
     * Get the probability of the attribute at the given time
     * @param elapsedTime the time elapsed from the start of the simulation
     * @return the probability of the attribute
     */
    fun getProbability(elapsedTime: Long): Double

    /**
     * Get the values of the attribute at the given time
     */
    fun getValues(elapsedTime: Long): Int

    companion object {
        /**
         * Calculate the probability of an attribute at the given time
         * @param endingValue the ending value
         * @param initialValue the initial value
         * @param delay the delay before the attribute starts to change
         * @param settlingDelay the delay before the attribute stops to change
         * @param elapsedTime the time elapsed from the start of the simulation
         * @param reverse if the probability should be reversed, in case of a Disappear (Decrease) behavior where the initial probability is the ending probability, and vice versa
         * @return the value of the attribute at the given elapsed time
         */
        inline fun <reified T: Number> calculateValue(
            endingValue: T,
            initialValue: T,
            delay: Long,
            settlingDelay: Long,
            elapsedTime: Long,
            reverse: Boolean = false
        ): T {
            if(endingValue == initialValue) return endingValue
            if (elapsedTime < delay) {
                return if (reverse) endingValue else initialValue
            }

            val effectiveTime = (elapsedTime - delay).coerceAtLeast(0)
            val totalSettlingTime = settlingDelay - delay

            return when {
                effectiveTime < totalSettlingTime -> {
                    val factor = if (reverse) 1 - effectiveTime.toDouble() / totalSettlingTime else effectiveTime.toDouble() / totalSettlingTime
                    when(endingValue) {
                        is Int -> round(endingValue.toDouble() * factor).toInt()
                        is Double -> endingValue.toDouble() * factor
                        else -> error("Unsupported type")
                    } as T
                }
                else -> if (reverse) initialValue else endingValue
            }
        }
    }
}

/**
 * Fixed behavior
 * @param probability the probability of the attribute
 */
@Serializable
@SerialName("Fixed")
data class Fixed(val probability: Double, val values: Int? = null) : Behavior {
    /**
     * Get the probability of the attribute at the given time
     * @param elapsedTime the time elapsed from the start of the simulation
     * @return the probability of the attribute
     */
    override fun getProbability(elapsedTime: Long): Double {
        return probability
    }

    override fun getValues(elapsedTime: Long): Int = values ?: throw IllegalStateException("Values not exists for measures.")
}

/**
 * Base interface for behaviors that involve changing probabilities over time.
 */
@Serializable
sealed interface TimeVaryingBehavior : Behavior {
    /**
     * The probability of the attribute at the start of the behavior
     */
    val startingProbability: Double

    /**
     * The probability of the attribute at the end of the behavior
     */
    val wantedProbability: Double

    /**
     * The delay before the attribute starts to change
     */
    val behaviorDelay: Long

    /**
     * The delay before the attribute stops to change
     */
    val settlingDelay: Long

    /**
     * The values of the attribute at the start of the behavior
     */
    val startingValues: Int

    /**
     * The values of the attribute at the end of the behavior
     */
    val wantedValues: Int

    fun validateProbabilities() {
        require(startingValues >= 0 && wantedValues >= 0) { "The starting values and wanted values must be greater than 0" }
        require(startingProbability in 0.0..1.0 && wantedProbability in 0.0..1.0) { "The probabilities must be between 0 and 1" }
        require(behaviorDelay >= 0 && settlingDelay >= 0) { "The delays must be positive" }
    }
}

/**
 * Increase probability behavior
 * @see Behavior
 */
@Serializable
sealed interface IncreaseBehavior : TimeVaryingBehavior {
    override fun getProbability(elapsedTime: Long): Double {
        return calculateValue(
            endingValue = wantedProbability,
            initialValue = startingProbability,
            delay = behaviorDelay,
            settlingDelay = settlingDelay,
            elapsedTime = elapsedTime
        )
    }

    override fun getValues(elapsedTime: Long): Int {
        return calculateValue(
            endingValue = wantedValues,
            initialValue = startingValues,
            delay = behaviorDelay,
            settlingDelay = settlingDelay,
            elapsedTime = elapsedTime
        )
    }

    override fun validateProbabilities() {
        super.validateProbabilities()
        require(startingProbability <= wantedProbability) { "The starting probability must be less or equal than the wanted probability" }
        require(startingValues <= wantedValues) { "The starting values must be less or equal than the wanted values" }
    }
}

/**
 * Appear behavior
 * @param probability the probability of the attribute at the end of the appearing behaviour
 * @param behaviorDelay the delay before the attribute appears
 * @param settlingDelay the delay before the attribute stops to appear
 * @see Behavior
 */
@Serializable
@SerialName("Appear")
data class Appear(val probability: Double, override val behaviorDelay: Long, override val settlingDelay: Long, val values: Int) :
    IncreaseBehavior {
    init {
        require(probability > 0) { "The probability must be greater than 0" }
        validateProbabilities()
    }
    override val startingProbability: Double = 0.0
    override val wantedProbability: Double = probability
    override val startingValues: Int = values
    override val wantedValues: Int = values
}

@Serializable
@SerialName("IncreaseProbability")
data class IncreaseProbability(override val startingProbability: Double, override val wantedProbability: Double, override val behaviorDelay: Long, override val settlingDelay: Long, val values: Int) :
    IncreaseBehavior {
    init {
        require(startingProbability < wantedProbability) { "The starting probability must be less than the wanted probability" }
        validateProbabilities()
    }

    override val startingValues: Int = values
    override val wantedValues: Int = values
}

@Serializable
@SerialName("IncreaseValues")
data class IncreaseValues(val probability: Double, override val behaviorDelay: Long, override val settlingDelay: Long, override val startingValues: Int, override val wantedValues: Int) :
    IncreaseBehavior {
    init {
        require(startingValues < wantedValues) { "The starting values must be less than the wanted values" }
        validateProbabilities()
    }

    override val startingProbability: Double = probability
    override val wantedProbability: Double = probability
}

/**
 * Decrease probability behavior
 * @see Behavior
 */
@Serializable
sealed interface DecreaseBehavior : TimeVaryingBehavior {

    override fun getProbability(elapsedTime: Long): Double {
        return calculateValue(
            endingValue = startingProbability,
            initialValue = wantedProbability,
            delay = behaviorDelay,
            settlingDelay = settlingDelay,
            elapsedTime = elapsedTime, reverse = true)
    }

    override fun getValues(elapsedTime: Long): Int {
        return calculateValue(
            endingValue = startingValues,
            initialValue = wantedValues,
            delay = behaviorDelay,
            settlingDelay = settlingDelay,
            elapsedTime = elapsedTime, reverse = true
        )
    }

    override fun validateProbabilities() {
        require(startingProbability >= wantedProbability) { "The starting probability must be greater or equal than the wanted probability" }
        require(startingValues >= wantedValues) { "The starting values must be greater or equal than the wanted values" }

        super.validateProbabilities()
    }
}

/**
 * Disappear behavior
 * @param probability the probability of the attribute before disappearing
 * @param behaviorDelay the delay before the attribute disappears
 * @param settlingDelay the delay before the attribute stops to disappear
 * @see Behavior
 */
@Serializable
@SerialName("Disappear")
data class Disappear(val probability: Double, override val behaviorDelay: Long, override val settlingDelay: Long, val values: Int) :
    DecreaseBehavior {
    init {
        require(probability > 0.0) { "The probability must be greater than 0" }
        validateProbabilities()
    }
    override val startingProbability: Double = probability
    override val wantedProbability: Double = 0.0

    override val startingValues: Int = values
    override val wantedValues: Int = values
}

/**
 * Decrease behavior
 * @param startingProbability the probability of the attribute before decreasing
 * @param wantedProbability the probability of the attribute after decreasing
 * @param behaviorDelay the delay before the attribute decreases
 * @param settlingDelay the delay before the attribute stops to decrease
 * @see Behavior
 */
@Serializable
@SerialName("DecreaseProbability")
data class DecreaseProbability(override val startingProbability: Double, override val wantedProbability: Double, override val behaviorDelay: Long, override val settlingDelay: Long, val values: Int) :
    DecreaseBehavior {
    init {
        require(startingProbability > wantedProbability) { "The starting probability must be greater than the wanted probability" }
        validateProbabilities()
    }

    override val startingValues: Int = values
    override val wantedValues: Int = values
}

@Serializable
@SerialName("DecreaseValues")
data class DecreaseValues(val probability: Double, override val behaviorDelay: Long, override val settlingDelay: Long, override val startingValues: Int, override val wantedValues: Int) :
    DecreaseBehavior {
    init {
        require(startingValues > wantedValues) { "The starting values must be greater than the wanted values" }
        validateProbabilities()
    }

    override val startingProbability: Double = probability
    override val wantedProbability: Double = probability
}