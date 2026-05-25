FROM eclipse-temurin:17-jre-jammy

WORKDIR /app
COPY target/stmnt-consistency-service.jar /app/app.jar

ENV JAVA_TOOL_OPTIONS="-XX:+UseG1GC -XX:MaxRAMPercentage=75"

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
