package it.unibo.big.analysis.configuration

import kotlinx.serialization.Serializable
import kotlin.math.round
import kotlin.reflect.KClass

@Serializable
/**
 * Class for define a random data variation
 * @param delay the delay of the variation before starting
 * @param frequency the frequency of the variation
 * @param extension the percentage of dimensions (random) involved in a (random) data variation behavior
 * @param impact the strength of the data variation behavior
 */
data class DataVariation(val delay: Long, val frequency: Long, val extension: Double, val impact: Double) {
    init {
        require(extension in 0.0..1.0) { "The percentage of dimensions must be in [0,1]" }
        require(impact in 0.0..1.0) { "The impact of dimensions must be in [0,1]" }
        require(frequency >= 0) { "The frequency must be >= 0" }
        require(delay >= 0) { "The delay must be >= 0" }
    }

    /**
     * @param elapsedTime the elapsed time
     * @return true if the data variation can be applied
     */
    private fun canApplyDataVariation(elapsedTime: Long): Boolean = elapsedTime % frequency == 0L && elapsedTime >= delay

    /**
     * @param elapsedTime the elapsed time
     * @param dimensions the starting dimensions
     * @return the updated dimensions considering the data variation parameters
     */
    fun getUpdatedBehaviours(elapsedTime: Long, dimensions: List<DimensionConfig>): List<DimensionConfig> {
        val numberOfDimensionsToSelect = round(dimensions.size * extension).toInt()

        val selectedBehaviours = if(canApplyDataVariation(elapsedTime)) dimensions.shuffled().take(numberOfDimensionsToSelect) else emptySet()

        return dimensions.map{
            if(selectedBehaviours.contains(it)) getRandomBehavior(it, elapsedTime) else it
        }
    }

    /**
     * Class for define a behavior info
     * @param behaviorClass the class of the behavior
     * @param changeValues if the values should be changed
     * @param increase if the values should be increased
     * @throws IllegalArgumentException if the parameters are not consistent
     *                                  (Boolean parameters are needed all and only for the Fixed behaviour.)
     */
    data class BehaviorInfo(val behaviorClass: KClass<*>, val changeValues: Boolean? = null, val increase: Boolean? = null) {
        init {
            if (changeValues != null || increase != null) require(changeValues != null && increase != null && behaviorClass == Fixed::class)
            { "Boolean parameters are needed all and only for the Fixed behaviour." }
        }
    }

    /**
     * @param dimensionConfig a dimension configuration
     * @param elapsedTime the elapsed time
     * @return the updated configuration for the given parameters
     */
    private fun getRandomBehavior(dimensionConfig: DimensionConfig, elapsedTime: Long): DimensionConfig {
        println("Selecting random behavior for ${dimensionConfig.name} at time $elapsedTime, previous behavior: ${dimensionConfig.behavior}")

        val values = dimensionConfig.behavior.getValues(elapsedTime)
        val probability = dimensionConfig.behavior.getProbability(elapsedTime)

        val decreaseProbabilityBehaviors = listOf(BehaviorInfo(DecreaseProbability::class), BehaviorInfo(Disappear::class), BehaviorInfo(Fixed::class, changeValues = false, increase = false))
        val increaseProbabilityBehaviors = listOf(BehaviorInfo(IncreaseProbability::class), BehaviorInfo(Appear::class), BehaviorInfo(Fixed::class, changeValues = false, increase = true))
        val increaseValuesBehaviors = listOf(BehaviorInfo(IncreaseValues::class), BehaviorInfo(Fixed::class, changeValues = true, increase = true))
        val decreaseValuesBehaviors = listOf(BehaviorInfo(DecreaseValues::class), BehaviorInfo(Fixed::class, changeValues = true, increase = false))

        val behaviorList = when {
            values == 1 && probability == 1.0 -> increaseValuesBehaviors + decreaseProbabilityBehaviors
            probability == 0.0 -> increaseProbabilityBehaviors
            probability == 1.0 -> decreaseProbabilityBehaviors + increaseValuesBehaviors + decreaseValuesBehaviors
            values == 1 -> increaseValuesBehaviors + decreaseProbabilityBehaviors + increaseProbabilityBehaviors
            else -> increaseProbabilityBehaviors + decreaseProbabilityBehaviors + increaseValuesBehaviors + decreaseValuesBehaviors
        }

        val randomBehavior = behaviorList.random()
        //set the behaviour delay to 0 and the settling delay to the frequency of the data variation
        val behaviorDelay = 0L
        val settlingDelay = frequency

        val newBehavior = when(randomBehavior.behaviorClass) {
            Fixed::class -> {
                if (randomBehavior.changeValues == true) {
                    Fixed(probability, calculateValue(values, randomBehavior.increase!!, 1))
                } else {
                    Fixed(calculateValue(probability, randomBehavior.increase!!, 0.0, 1.0), values)
                }
            }
            Appear::class -> Appear(
                calculateValue(probability, true, 0.0, 1.0),
                behaviorDelay, settlingDelay, values
            )
            IncreaseProbability::class -> IncreaseProbability(
                probability,
                calculateValue(probability, true, 0.0, 1.0),
                behaviorDelay, settlingDelay, values
            )
            IncreaseValues::class ->
                IncreaseValues(probability, behaviorDelay, settlingDelay, values, calculateValue(values, true, 1))
            Disappear::class ->
                Disappear(
                    calculateValue(probability, false, 0.0, 1.0),
                    behaviorDelay, settlingDelay, values
                )
            DecreaseProbability::class ->
                DecreaseProbability(
                    probability,
                    calculateValue(probability, false, 0.0, 1.0),
                    behaviorDelay, settlingDelay, values
                )
            DecreaseValues::class ->
                DecreaseValues(probability, behaviorDelay, settlingDelay, values, calculateValue(values, false, 1))
            else -> error("Undefined behaviour class")
        }
        println("selected behavior ${newBehavior::class}: $newBehavior")
        return dimensionConfig.copy(behavior = newBehavior)
    }

    /**
     * @param value the starting value
     * @param increase true if you want to increase the value considering the impact, false if you want to decrease
     * @param lowerBound the lower bound of the returned value, if the value is int should be 0 else >= 0
     * @param upperBound the lower bound of the returned value, if the value is int should be null else >= lowerBound
     */
    private inline fun <reified T: Number> calculateValue(value: T, increase: Boolean, lowerBound: T, upperBound: T? = null): T {
        return when(value) {
            is Int -> {
                var newValue = round(value + (if(increase) (value * impact) else (value * -impact))).toInt()

                require(upperBound == null) { "In case of an integer no upper bound should be defined"}
                require(lowerBound.toInt() == 1) { "In case of an integer the lower bound should be zero" }
                if(newValue == value.toInt()) { newValue = if(increase) value + 1 else value - 1 }
                if(newValue < lowerBound.toInt()) { newValue = lowerBound.toInt() }
                newValue
            }
            is Double -> {
                var newValue = if(value == lowerBound) (if(increase) impact else -impact) else (value + (if(increase) (value * impact) else (value * -impact)))
                require(lowerBound.toDouble() >= 0) { "In case of a double the lower bound should be >= 0"}
                require(upperBound != null && upperBound.toDouble() >= lowerBound.toDouble()) { "In case of a double an upper bound should be defined and >= lower bound"}
                if(newValue > upperBound.toDouble()) { newValue = upperBound.toDouble() }
                if(newValue < lowerBound.toDouble()) { newValue = lowerBound.toDouble() }
                newValue
            }
            else -> error("Unsupported type")
        } as T
    }
}
