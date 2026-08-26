# Dockerfile - BAMOE Camel DB2-Kafka Integration (TLS-enabled)
# =============================================================================
# Base image - Eclipse Temurin JDK 17
FROM eclipse-temurin:17-jre-jammy

# Maintainer info
LABEL maintainer="araghuna@dcss.gov"
LABEL description="DCSS BAMOE Camel DB2-Kafka Integration - TLS"
LABEL version="1.0.0"

# Set working directory inside the container
WORKDIR /app

# Copy the Spring Boot fat jar
COPY target/camel-db2-kafka-1.0.0.jar ./camel-db2-kafka-1.0.0.jar

# Copy the DCSS internal CA truststore (contains root, intermediate, and leaf certs
# for vmdevtestdboltp02.dcss.gov, required since DB2 enforces TLS as of the
# July 2026 migration). Baking this in means any teammate can run the image
# unchanged, without needing to generate their own truststore.
COPY dcss-truststore.jks ./dcss-truststore.jks

# Create logs directory
RUN mkdir -p /app/logs

# Expose port 8080 - the port Camel runs on
EXPOSE 8080

# Start the Spring Boot application
ENTRYPOINT ["java", \
            "-Djava.security.egd=file:/dev/./urandom", \
            "-jar", \
            "camel-db2-kafka-1.0.0.jar"]