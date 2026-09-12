FROM eclipse-temurin:21.0.9_10-jdk AS build
WORKDIR /app
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY engine engine
COPY benchmarks benchmarks
RUN ./mvnw -B -ntp -DskipTests -DskipITs package
FROM eclipse-temurin:21.0.9_10-jre
WORKDIR /app
RUN groupadd --system tickforge && useradd --system --gid tickforge tickforge
COPY --from=build /app/engine/target/engine-1.0.0-SNAPSHOT.jar /app/engine.jar
COPY config /app/config
USER tickforge
ENTRYPOINT ["java", "-jar", "/app/engine.jar"]
