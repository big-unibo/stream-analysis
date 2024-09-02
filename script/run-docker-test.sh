#!/bin/sh

# run simulator
java -cp simulator/build/libs/simulator-0.1-all.jar it.unibo.big.analysis.simulation.GeneratorKt

# run test
java -cp dataconsumer/build/libs/dataconsumer-0.1-all.jar it.unibo.big.query.app.Test

# run the test
for test in dataconsumer/src/main/python/it/big/unibo/query/*.py; do
    # if not common
    if [ "$test" != "dataconsumer/src/main/python/it/big/unibo/query/common.py" ]; then
        python3 "$test"
    fi
done