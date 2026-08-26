package com.example.camel.config;

import com.example.camel.CamelApplication;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;

// Runs automatically on startup to test DB2 and Kafka connectivity
// confirms both are reachable before any real request arrives from BAMOE
@Component
public class BamoeConnectionTester {

    // DB2 datasource and Kafka properties needed for the connection tests
    private final DataSource caseDb2DataSource;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${kafka.bamoe.publish.topic}")
    private String kafkaPublishTopic;

    @Value("${kafka.orch.error.topic}")
    private String kafkaErrorTopic;

    public BamoeConnectionTester(
            @Qualifier("caseDb2DataSource") DataSource caseDb2DataSource) {
        this.caseDb2DataSource = caseDb2DataSource;
    }

    // Runs automatically once the app is fully started
    // triggers both DB2 and Kafka connection tests
    @EventListener(ApplicationReadyEvent.class)
    public void runConnectionTests() {
        CamelApplication.log("INFO",
            "[BAMOE-CONNECTION-TESTER] ========================================");
        CamelApplication.log("INFO",
            "[BAMOE-CONNECTION-TESTER] Running startup connection tests...");
        CamelApplication.log("INFO",
            "[BAMOE-CONNECTION-TESTER] ========================================");

        testDb2Connection();
        testKafkaConnection();

        CamelApplication.log("INFO",
            "[BAMOE-CONNECTION-TESTER] ========================================");
        CamelApplication.log("INFO",
            "[BAMOE-CONNECTION-TESTER] Connection tests completed.");
        CamelApplication.log("INFO",
            "[BAMOE-CONNECTION-TESTER] ========================================");
    }

    // Opens a DB2 connection and runs a lightweight ping query to confirm DB2 is reachable
    private void testDb2Connection() {
        CamelApplication.log("INFO",
            "[BAMOE-CONNECTION-TESTER] [DB2] Testing DB2 connection...");

        Connection connection = null;
        Statement  statement  = null;
        ResultSet  resultSet  = null;

        try {
            connection = caseDb2DataSource.getConnection();
            CamelApplication.log("INFO",
                "[BAMOE-CONNECTION-TESTER] [DB2] Connection opened successfully");

            statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT 1 FROM SYSIBM.SYSDUMMY1");

            if (resultSet.next()) {
                CamelApplication.log("INFO",
                    "[BAMOE-CONNECTION-TESTER] [DB2] - DB2 CONNECTION SUCCESS"
                    + " | Ping query returned: " + resultSet.getInt(1)
                    + " | DB2 is reachable and responding");
            }
        } catch (Exception e) {
            String reason = e.getMessage();
            if (reason != null && reason.contains("\n")) {
                reason = reason.substring(0, reason.indexOf("\n"));
            }
            CamelApplication.log("ERROR",
                "[BAMOE-CONNECTION-TESTER] [DB2] - DB2 CONNECTION FAILED"
                + " | Reason=" + reason
                + " | Check: server, port, username, password in application.properties");
        } finally {
            // Close all resources regardless of success or failure
            try { if (resultSet  != null) resultSet.close();  } catch (Exception ignored) {}
            try { if (statement  != null) statement.close();  } catch (Exception ignored) {}
            try { if (connection != null) connection.close(); } catch (Exception ignored) {}
        }
    }

    // Connects to the Kafka broker and checks if both topics exist
    private void testKafkaConnection() {
        CamelApplication.log("INFO",
            "[BAMOE-CONNECTION-TESTER] [KAFKA] Testing Kafka connection..."
            + " | Broker=" + kafkaBootstrapServers);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");

        try (AdminClient adminClient = AdminClient.create(props)) {

            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topicNames = topicsResult.names().get();

            CamelApplication.log("INFO",
                "[BAMOE-CONNECTION-TESTER] [KAFKA] - KAFKA BROKER CONNECTION SUCCESS"
                + " | Broker=" + kafkaBootstrapServers
                + " | Total topics on broker=" + topicNames.size());

            // Check if bamoe-event-publish topic exists - used for request and success events
            if (topicNames.contains(kafkaPublishTopic)) {
                CamelApplication.log("INFO",
                    "[BAMOE-CONNECTION-TESTER] [KAFKA] - Topic EXISTS"
                    + " | Topic=" + kafkaPublishTopic);
            } else {
                CamelApplication.log("ERROR",
                    "[BAMOE-CONNECTION-TESTER] [KAFKA] - Topic NOT FOUND"
                    + " | Topic=" + kafkaPublishTopic
                    + " | Action: Create this topic on the Kafka broker");
            }

            // Check if orch-engine-error-log topic exists - used for all error events
            if (topicNames.contains(kafkaErrorTopic)) {
                CamelApplication.log("INFO",
                    "[BAMOE-CONNECTION-TESTER] [KAFKA] - Topic EXISTS"
                    + " | Topic=" + kafkaErrorTopic);
            } else {
                CamelApplication.log("ERROR",
                    "[BAMOE-CONNECTION-TESTER] [KAFKA] - Topic NOT FOUND"
                    + " | Topic=" + kafkaErrorTopic
                    + " | Action: Create this topic on the Kafka broker");
            }

        } catch (Exception e) {
            String reason = e.getMessage();
            if (reason != null && reason.contains("\n")) {
                reason = reason.substring(0, reason.indexOf("\n"));
            }
            CamelApplication.log("ERROR",
                "[BAMOE-CONNECTION-TESTER] [KAFKA] - KAFKA BROKER CONNECTION FAILED"
                + " | Broker=" + kafkaBootstrapServers
                + " | Reason=" + reason
                + " | Check: broker IP and port in application.properties");
        }
    }
}