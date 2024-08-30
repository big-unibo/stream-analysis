FROM openjdk:8-jdk-slim

WORKDIR /app

COPY gradlew /app/gradlew
COPY gradle /app/gradle
COPY build.gradle /app/build.gradle
COPY settings.gradle /app/settings.gradle
COPY gradle.properties /app/gradle.properties
COPY config/ /app/config
COPY dataconsumer/ /app/dataconsumer
COPY simulator/ /app/simulator
COPY script/ /app/script
#create directory app test
RUN mkdir /app/test
RUN mkdir /app/test/graphs
RUN mkdir /app/test/tables
RUN mkdir /app/logs

RUN chmod +x gradlew

# create jars
RUN ./gradlew simulator:shadowJar --no-daemon -Dorg.gradle.daemon=false -Dorg.gradle.vfs.watch=false
RUN ./gradlew dataconsumer:shadowJar --no-daemon -Dorg.gradle.daemon=false -Dorg.gradle.vfs.watch=false
# run tests
CMD ["script/run-docker-test.sh"]
