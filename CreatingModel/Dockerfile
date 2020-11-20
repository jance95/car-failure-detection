FROM openjdk:8-jdk-alpine

ARG JAR_FILE=target/find-links.jar
ARG JAR_LIB_FILE=target/lib/
ARG TEST_DATA=target/testdata.txt
# cd /usr/local/runme
WORKDIR /usr/local/runme

# copy target/find-links.jar /usr/local/runme/app.jar
COPY ${JAR_FILE} app.jar

# copy project dependencies
# cp -rf target/lib/  /usr/local/runme/lib
ADD ${JAR_LIB_FILE} lib/

# cp -rf target/testdata.txt  /usr/local/runme/testdata.txt
COPY ${TEST_DATA} testdata.txt
#copy project configuration
# java -jar /usr/local/runme/app.jar
ENTRYPOINT ["java","-jar","app.jar"]