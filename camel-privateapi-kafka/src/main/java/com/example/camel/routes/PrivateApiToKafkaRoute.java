package com.example.camel.routes;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class PrivateApiToKafkaRoute extends RouteBuilder {

    // Business-only logger (goes to console + file)
    private static final Logger businessLog =
            LoggerFactory.getLogger("BUSINESS_LOG");

    // Timestamp format
    private static final DateTimeFormatter TS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    public void configure() {

        /* =====================================================
           Global Exception Handling
           ===================================================== */
        onException(Exception.class)
            .handled(true)
            .process(exchange -> {

                Exception ex = exchange.getProperty(
                        Exchange.EXCEPTION_CAUGHT, Exception.class);

                String msg = ex != null ? ex.getMessage() : "Unknown error";
                String ts = LocalDateTime.now().format(TS_FORMAT);

                // ---- Kafka related failures ----
                if (msg != null && (
                        msg.contains("Broker may not be available")
                     || msg.contains("Failed to update metadata")
                     || msg.contains("Connection to node")
                     || msg.contains("Timeout"))) {

                    System.out.println("Kafka Error: " + msg);



                    businessLog.error(
                        "[{}] ERROR_OCCURRED => Kafka broker is DOWN or unreachable",
                        ts);

                }

                // ---- Private API failures ----
                else if (msg != null && (
                        msg.contains("Connection refused")
                     || msg.contains("Read timed out")
                     || msg.contains("404")
                     || msg.contains("500"))) {
                        System.out.println("Private API Error: " + msg);

                    businessLog.error(
                        "[{}] ERROR_OCCURRED => Private API is DOWN or unreachable",
                        ts);
                }
                // ---- Generic fallback ----
                else {
                    businessLog.error(
                        "[{}] ERROR_OCCURRED => {}",
                        ts, msg);
                }
            });

        /* =====================================================
           Main Route
           ===================================================== */
        from("timer:privateApiPoller?period=5000")
            .routeId("private-api-to-kafka-route")

            // ---- Capture timestamp at route start ----
            .process(e -> {
                String ts = LocalDateTime.now().format(TS_FORMAT);
                e.getIn().setHeader("eventTimestamp", ts);

                businessLog.info(
                    "[{}] API_CALL_STARTED => Calling Private API",
                    ts);
            })

            .setHeader(Exchange.HTTP_METHOD, constant("GET"))
            .toD("{{private.api.url}}")

            // ---- API response received ----
            .process(e -> {
                String body = e.getIn().getBody(String.class);
                String ts = e.getIn().getHeader("eventTimestamp", String.class);

                businessLog.info(
                    "[{}] API_RESPONSE_RECEIVED => {}",
                    ts, body);
            })

            // ---- Message ready for Kafka ----
            .process(e -> {
                String ts = e.getIn().getHeader("eventTimestamp", String.class);

                businessLog.info(
                    "[{}] MESSAGE_READY_FOR_KAFKA => Payload prepared successfully",
                    ts);
            })

            // ---- Send timestamp as Kafka header ----
            .setHeader("kafka_event_time",
                header("eventTimestamp"))

            // ---- Send to Kafka ----
            .toD("kafka:{{kafka.topic}}"
                + "?brokers={{kafka.bootstrap.servers}}"
                + "&configuration.retries=3")

            // ---- Message sent successfully ----
            .process(e -> {
                String body = e.getIn().getBody(String.class);
                String ts = e.getIn().getHeader("eventTimestamp", String.class);

                businessLog.info(
                    "[{}] MESSAGE_SENT_TO_KAFKA => {}",
                    ts, body);
            });
    }
}
