FROM openjdk:21-jdk-slim

WORKDIR /app

COPY gradlew /app/gradlew
COPY gradle /app/gradle
COPY build.gradle /app/build.gradle
COPY settings.gradle /app/settings.gradle
COPY gradle.properties /app/gradle.properties
COPY config/ /app/config
COPY algorithms/ /app/algorithms
COPY generator/ /app/generator
COPY script/ /app/script
#create directory app test
RUN mkdir /app/test
RUN mkdir /app/test/graphs
RUN mkdir /app/test/tables
RUN mkdir /app/logs

RUN apt-get update && \
    apt-get install -y python3 python3-pip dvipng ghostscript \
    texlive-fonts-recommended texlive-latex-base texlive-latex-extra \
    texlive-latex-recommended texlive-publishers texlive-science \
    texlive-xetex cm-super gcc libpq-dev

# activate conda environment with algorithms/src/main/python/requirements.txt
RUN pip3 install -r algorithms/src/main/python/requirements.txt

RUN chmod +x gradlew
# create jars
RUN ./gradlew generator:shadowJar --no-daemon -Dorg.gradle.daemon=false -Dorg.gradle.vfs.watch=false
RUN ./gradlew algorithms:shadowJar --no-daemon -Dorg.gradle.daemon=false -Dorg.gradle.vfs.watch=false
# run tests
CMD ["script/run-docker-test.sh"]
