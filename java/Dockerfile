FROM maven:3.9.11-eclipse-temurin-21-alpine AS builder
WORKDIR /app
COPY . /app

RUN mvn clean install

FROM gcr.io/distroless/java21-debian12
WORKDIR /app
COPY --from=builder /app/target/*-jar-with-dependencies.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
