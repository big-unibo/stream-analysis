#!/bin/sh

# run simulator
java -cp simulator/build/libs/simulator-0.1-all.jar it.unibo.big.analysis.simulation.GeneratorKt

# run test
java -cp dataconsumer/build/libs/dataconsumer-0.1-all.jar it.unibo.big.query.app.Test

# python code to generate the graph
apt-get update && apt-get install -y python3 python3-pip
# activate conda environment with dataconsumer/src/main/python/requirements.txt
pip3 install -r dataconsumer/src/main/python/requirements.txt
python3 dataconsumer/src/main/python/it/big/unibo/query/paperTest.py