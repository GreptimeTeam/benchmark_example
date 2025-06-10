FROM maven:3.8.7-eclipse-temurin-17 AS builder

COPY pom.xml ./

RUN mvn dependency:go-offline

WORKDIR /app
COPY . /app
RUN mvn clean package

FROM openjdk:8-jdk-slim
WORKDIR /app
COPY --from=builder /app/target/benchmark_example.jar /app/benchmark_example.jar
ENTRYPOINT ["java", "-jar", "/app/benchmark_example.jar"]