# ---- Stage 1: build ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests package

# ---- Stage 2: runtime ----
FROM eclipse-temurin:21-jre
WORKDIR /app

# Pull the OTel Java auto-instrumentation agent. No code changes needed in the app:
# this agent hooks JDBC, Tomcat, HTTP client, Logback, etc.
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.7.0/opentelemetry-javaagent.jar /otel/javaagent.jar

COPY --from=build /src/target/health-api.jar /app/health-api.jar

EXPOSE 8080
ENTRYPOINT ["sh", "-c", "java -javaagent:/otel/javaagent.jar -jar /app/health-api.jar"]
