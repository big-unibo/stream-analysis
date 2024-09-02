# BIG - Stream Analysis

[![build](https://github.com/big-unibo/stream-analysis#big---project-template-stream-analysis/actions/workflows/build.yml/badge.svg)](https://github.com/big-unibo/stream-analysis#big---project-template-stream-analysis/actions/workflows/build.yml)

## Multi-project gradle structure
    simulator/   -- generator of stream data
    dataconsumer/  -- stream analysis algorithms implementations
    test/ -- test results
### Simulator
The main class is `it.unibo.big.analysis.simulation.GeneratorKt` (in Kotlin),
it generates the datasets and write them in test folder.

### DataConsumer
The main class is `it.unibo.big.query.app.Test` (in Scala), it runs tests on the datasets generated by the simulator.
Then the results are stored in test folder and insights are retrieved with python code in `dataconsumer/src/main/python/it/big/unibo/query/paperTest.py`.

## How to run
It requires docker installed and running.

For run the test and obtain the results in the test folder, you can run the following command:

Linux:
```shell
./test.sh
```

Windows:
```shell
.\test.bat
```