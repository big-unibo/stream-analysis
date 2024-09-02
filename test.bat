@echo off

REM Build the Docker image with the tag 'stream-analysis'
docker build -t stream-analysis .

REM Create necessary directories if they don't exist
IF NOT EXIST "%cd%\test" mkdir "%cd%\test"
IF NOT EXIST "%cd%\test\graphs" mkdir "%cd%\test\graphs"
IF NOT EXIST "%cd%\test\tables" mkdir "%cd%\test\tables"
IF NOT EXIST "%cd%\logs" mkdir "%cd%\logs"

REM Run the Docker container, mounting the test and logs directories
docker run -v "%cd%\test:/app/test" -v "%cd%\logs:/app/logs" stream-analysis
