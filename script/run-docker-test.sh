#!/bin/sh

# run generator
java -cp generator/build/libs/generator-0.1-all.jar it.unibo.big.streamanalysis.generator.DatasetsGeneratorKt

# run test
java -cp algorithms/build/libs/algorithms-0.1-all.jar it.unibo.big.streamanalysis.algorithm.app.AlgorithmsExecution

# run the test
for test in algorithms/src/main/python/it/big/unibo/query/*.py; do
    # if not common
    if [ "$test" != "algorithms/src/main/python/it/big/unibo/query/common.py" ]; then
        python3 "$test"
    fi
done