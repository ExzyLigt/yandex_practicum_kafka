# Stage 1: build
FROM maven:3.9.9-eclipse-temurin-24 AS build
WORKDIR /workspace
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

# Stage 2: runtime
FROM eclipse-temurin:24-jre
WORKDIR /app
COPY --from=build /workspace/target/kafka-app-1.0-SNAPSHOT.jar /app/kafka-app.jar
ENTRYPOINT ["java","-jar","/app/kafka-app.jar"]
