FROM maven:3.9-eclipse-temurin-21 AS build

RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app/java

COPY java/pom.xml ./
RUN mvn -q -B dependency:copy-dependencies -DoutputDirectory=target/lib

COPY proto/ ../proto/
COPY java/src ./src
RUN protoc --proto_path=../proto --java_out=src/main/java ../proto/chat.proto \
 && mvn -q -B compile

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /app/java/target/classes ./classes
COPY --from=build /app/java/target/lib ./lib

CMD ["java", "-cp", "classes:lib/*", "bbs.Server"]
