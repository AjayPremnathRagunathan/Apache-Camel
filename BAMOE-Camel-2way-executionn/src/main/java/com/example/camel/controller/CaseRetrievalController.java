package com.example.camel.controller;

import com.example.camel.CamelApplication;
import com.example.camel.connector.CamelDb2Connector;
import com.example.camel.registry.CamelQueryRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// Spring REST Controller that accepts POST requests from BAMOE/Postman.
//
// /execute-query  -> FETCH / SELECT
// /execute-insert -> INSERT
// /execute-update -> UPDATE
//
// queryId identifies the SQL in CamelQueryRegistry.
// No query-specific INSERT/UPDATE logic is hardcoded here.
@RestController
@RequestMapping("/bamoe")
public class CaseRetrievalController {

    // Kafka topic for request received and success response events
    private static final String KAFKA_PUBLISH_TOPIC =
        "bamoe-event-publish";

    // Kafka topic for all error events
    private static final String KAFKA_ERROR_TOPIC =
        "orch-engine-error-log";

    // DB2 connector handles database connection and execution
    @Autowired
    private CamelDb2Connector camelDb2Connector;

    // Query registry holds SQL queries mapped to queryId
    @Autowired
    private CamelQueryRegistry camelQueryRegistry;

    // Camel ProducerTemplate used to publish events to Kafka
    @Autowired
    private ProducerTemplate producerTemplate;

    // Kafka broker address
    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    // Jackson parser.
    // Spring Boot normally provides this through spring-web.
    @Autowired
    private ObjectMapper objectMapper;

    // ============================================================
    // FETCH / SELECT
    // ============================================================

    @PostMapping("/execute-query")
    public ResponseEntity<String> executeQuery(
            @RequestBody String requestBody) {

        Instant start = Instant.now();

        String caseId = "UNKNOWN";
        String queryId = "UNKNOWN";

        CamelApplication.log(
            "INFO",
            "[BAMOE-CAMEL-DB2] STEP 1 - "
            + "HTTP POST Request Received from BAMOE"
            + " | Body=" + requestBody
        );

        try {

            // ----------------------------------------------------
            // STEP 1 - Parse request
            // ----------------------------------------------------

            JsonNode root =
                objectMapper.readTree(requestBody);

            queryId =
                getRequiredText(
                    root,
                    "queryId"
                );

            if (queryId == null
                    || queryId.isBlank()) {

                return buildErrorResponse(
                    400,
                    "UNKNOWN",
                    "UNKNOWN",
                    "Missing queryId field in request body."
                    + " Available queryIds: "
                    + camelQueryRegistry.getAvailableQueryIds(),
                    start
                );
            }

            JsonNode paramsNode =
                root.get("params");

            if (paramsNode == null
                    || !paramsNode.isObject()) {

                return buildErrorResponse(
                    400,
                    queryId,
                    "UNKNOWN",
                    "Missing params field in request body.",
                    start
                );
            }

            caseId =
                getRequiredText(
                    paramsNode,
                    "caseId"
                );

            if (caseId == null
                    || caseId.isBlank()) {

                return buildErrorResponse(
                    400,
                    queryId,
                    "UNKNOWN",
                    "Missing params.caseId field in request body.",
                    start
                );
            }

            // ----------------------------------------------------
            // Look up SQL dynamically using queryId
            // ----------------------------------------------------

            String sqlQuery =
                camelQueryRegistry.getQuery(
                    queryId
                );

            if (sqlQuery == null) {

                return buildErrorResponse(
                    400,
                    queryId,
                    caseId,
                    "Unknown queryId: " + queryId
                    + ". Available queryIds: "
                    + camelQueryRegistry.getAvailableQueryIds(),
                    start
                );
            }

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2] STEP 1 - Parsed successfully"
                + " | queryId=" + queryId
                + " | caseId=" + caseId
            );

            // ----------------------------------------------------
            // STEP 2 - Kafka REQUEST event
            // ----------------------------------------------------

            String kafkaRequestPayload =
                "{"
                + "\"event\":\"BAMOE_CAMEL_CASE_REQUEST_RECEIVED\","
                + "\"message\":\"Camel has received the Case ID "
                + escapeJson(caseId)
                + " from BAMOE.\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"timestamp\":\""
                + Instant.now().toString()
                + "\""
                + "}";

            publishToKafka(
                KAFKA_PUBLISH_TOPIC,
                kafkaRequestPayload
            );

            // ----------------------------------------------------
            // STEP 3 - Execute SELECT
            // ----------------------------------------------------

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2] STEP 3 - "
                + "Calling CamelDb2Connector"
                + " | queryId=" + queryId
                + " | caseId=" + caseId
            );

            List<Map<String, Object>> rows =
                camelDb2Connector.executeQuery(
                    sqlQuery,
                    caseId.trim()
                );

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2] STEP 3 - DB2 data received"
                + " | queryId=" + queryId
                + " | caseId=" + caseId
                + " | RowsReturned="
                + (rows == null ? 0 : rows.size())
            );

            // ----------------------------------------------------
            // STEP 4 - Build response
            // ----------------------------------------------------

            if (rows == null
                    || rows.isEmpty()) {

                String message =
                    "Case ID " + caseId
                    + " is invalid or not found in DB2."
                    + " Please verify the Case ID and try again.";

                publishToKafka(
                    KAFKA_ERROR_TOPIC,
                    "{"
                    + "\"event\":\"BAMOE_CAMEL_CASE_ERROR\","
                    + "\"queryId\":\""
                    + escapeJson(queryId)
                    + "\","
                    + "\"caseId\":\""
                    + escapeJson(caseId)
                    + "\","
                    + "\"message\":\""
                    + escapeJson(message)
                    + "\","
                    + "\"timestamp\":\""
                    + Instant.now().toString()
                    + "\""
                    + "}"
                );

                CamelApplication.logOperation(
                    "SELECT",
                    queryId,
                    "ERROR",
                    message
                );

                return ResponseEntity.status(404)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(
                        "{"
                        + "\"status\":\"ERROR\","
                        + "\"queryId\":\""
                        + escapeJson(queryId)
                        + "\","
                        + "\"caseId\":\""
                        + escapeJson(caseId)
                        + "\","
                        + "\"message\":\""
                        + escapeJson(message)
                        + "\""
                        + "}"
                    );
            }

            StringBuilder dataArray =
                new StringBuilder("[");

            for (int i = 0;
                 i < rows.size();
                 i++) {

                Map<String, Object> row =
                    rows.get(i);

                if (i > 0) {
                    dataArray.append(",");
                }

                dataArray.append("{");

                boolean firstCol = true;

                for (Map.Entry<String, Object> entry
                        : row.entrySet()) {

                    if (!firstCol) {
                        dataArray.append(",");
                    }

                    dataArray
                        .append("\"")
                        .append(
                            escapeJson(entry.getKey())
                        )
                        .append("\":")
                        .append(
                            toJsonValue(
                                entry.getValue()
                            )
                        );

                    firstCol = false;
                }

                dataArray.append("}");
            }

            dataArray.append("]");

            String jsonResponse =
                "{"
                + "\"status\":\"SUCCESS\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"caseId\":\""
                + escapeJson(caseId)
                + "\","
                + "\"totalRecords\":"
                + rows.size()
                + ","
                + "\"data\":"
                + dataArray
                + "}";

            long ms =
                Instant.now().toEpochMilli()
                - start.toEpochMilli();

            publishToKafka(
                KAFKA_PUBLISH_TOPIC,
                "{"
                + "\"event\":\"DB2_CAMEL_BAMOE_CASE_DATA_SENT\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"message\":\"Data for Case ID "
                + escapeJson(caseId)
                + " has been extracted from DB2 and sent to BAMOE.\","
                + "\"timestamp\":\""
                + Instant.now().toString()
                + "\""
                + "}"
            );

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2] ROUTE COMPLETE"
                + " | queryId=" + queryId
                + " | caseId=" + caseId
                + " | DurationMs=" + ms
            );

            CamelApplication.logOperation(
                "SELECT",
                queryId,
                "SUCCESS",
                "RowsReturned=" + rows.size()
                + " | CaseId=" + caseId
            );

            return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(jsonResponse);

        } catch (Exception e) {

            String reason =
                cleanErrorMessage(e);

            long ms =
                Instant.now().toEpochMilli()
                - start.toEpochMilli();

            CamelApplication.log(
                "ERROR",
                "[BAMOE-CAMEL-DB2] ROUTE FAILED"
                + " | queryId=" + queryId
                + " | caseId=" + caseId
                + " | Reason=" + reason
                + " | DurationMs=" + ms
            );

            CamelApplication.logOperation(
                "SELECT",
                queryId,
                "ERROR",
                reason
            );

            publishToKafka(
                KAFKA_ERROR_TOPIC,
                "{"
                + "\"event\":\"BAMOE_CAMEL_ROUTE_ERROR\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"caseId\":\""
                + escapeJson(caseId)
                + "\","
                + "\"message\":\""
                + escapeJson(reason)
                + "\","
                + "\"timestamp\":\""
                + Instant.now().toString()
                + "\""
                + "}"
            );

            return ResponseEntity.status(500)
                .contentType(MediaType.APPLICATION_JSON)
                .body(
                    "{"
                    + "\"status\":\"ERROR\","
                    + "\"queryId\":\""
                    + escapeJson(queryId)
                    + "\","
                    + "\"caseId\":\""
                    + escapeJson(caseId)
                    + "\","
                    + "\"message\":\""
                    + escapeJson(reason)
                    + "\""
                    + "}"
                );
        }
    }

    // ============================================================
    // INSERT
    // ============================================================

    @PostMapping("/execute-insert")
    public ResponseEntity<String> executeInsert(
            @RequestBody String requestBody) {

        Instant start = Instant.now();

        String queryId = "UNKNOWN";

        CamelApplication.log(
            "INFO",
            "[BAMOE-CAMEL-DB2-INSERT] STEP 1 - "
            + "HTTP POST Request Received"
            + " | Body=" + requestBody
        );

        try {

            // ----------------------------------------------------
            // STEP 1 - Parse JSON
            // ----------------------------------------------------

            JsonNode root =
                objectMapper.readTree(requestBody);

            queryId =
                getRequiredText(
                    root,
                    "queryId"
                );

            if (queryId == null
                    || queryId.isBlank()) {

                return buildInsertErrorResponse(
                    400,
                    "UNKNOWN",
                    "Missing queryId field in request body."
                    + " Available queryIds: "
                    + camelQueryRegistry.getAvailableQueryIds(),
                    start
                );
            }

            JsonNode paramsNode =
                root.get("params");

            if (paramsNode == null
                    || !paramsNode.isObject()) {

                return buildInsertErrorResponse(
                    400,
                    queryId,
                    "Missing params field in request body.",
                    start
                );
            }

            // ----------------------------------------------------
            // STEP 2 - Get SQL from registry
            // ----------------------------------------------------

            String sqlQuery =
                camelQueryRegistry.getQuery(
                    queryId
                );

            if (sqlQuery == null) {

                return buildInsertErrorResponse(
                    400,
                    queryId,
                    "Unknown queryId: " + queryId
                    + ". Available queryIds: "
                    + camelQueryRegistry.getAvailableQueryIds(),
                    start
                );
            }

            // ----------------------------------------------------
            // Validate that the SQL is actually INSERT.
            //
            // This prevents someone from accidentally sending
            // a SELECT queryId to /execute-insert.
            //
            // This does NOT check for specific query IDs.
            // ----------------------------------------------------

            if (!isInsertSql(sqlQuery)) {

                return buildInsertErrorResponse(
                    400,
                    queryId,
                    "The queryId '" + queryId
                    + "' does not reference an INSERT statement."
                    + " /execute-insert can only execute INSERT SQL.",
                    start
                );
            }

            // ----------------------------------------------------
            // Convert params JSON into a Map.
            //
            // LinkedHashMap preserves JSON order, but the connector
            // will ultimately use SQL column names, so order does
            // not matter.
            // ----------------------------------------------------

            Map<String, Object> params =
                objectMapper.convertValue(
                    paramsNode,
                    new TypeReference<
                        LinkedHashMap<String, Object>
                    >() {}
                );

            if (params.isEmpty()) {

                return buildInsertErrorResponse(
                    400,
                    queryId,
                    "The params object cannot be empty.",
                    start
                );
            }

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-INSERT] STEP 1 - Parsed successfully"
                + " | queryId=" + queryId
                + " | ParameterCount=" + params.size()
            );

            // ----------------------------------------------------
            // STEP 3 - Kafka REQUEST event
            // ----------------------------------------------------

            String kafkaRequestPayload =
                "{"
                + "\"event\":\"BAMOE_CAMEL_INSERT_REQUEST_RECEIVED\","
                + "\"message\":\"Camel has received an INSERT request.\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"timestamp\":\""
                + Instant.now().toString()
                + "\""
                + "}";

            publishToKafka(
                KAFKA_PUBLISH_TOPIC,
                kafkaRequestPayload
            );

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-INSERT] STEP 2 - "
                + "Kafka REQUEST event published"
                + " | Topic=" + KAFKA_PUBLISH_TOPIC
                + " | queryId=" + queryId
            );

            // ----------------------------------------------------
            // STEP 4 - Execute INSERT
            // ----------------------------------------------------

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-INSERT] STEP 3 - "
                + "Calling CamelDb2Connector"
                + " | queryId=" + queryId
            );

            int rowsInserted =
                camelDb2Connector.executeInsert(
                    sqlQuery,
                    params
                );

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-INSERT] STEP 3 - "
                + "DB2 INSERT complete"
                + " | queryId=" + queryId
                + " | RowsInserted=" + rowsInserted
            );

            // ----------------------------------------------------
            // STEP 5 - Build response
            // ----------------------------------------------------

            long ms =
                Instant.now().toEpochMilli()
                - start.toEpochMilli();

            String jsonResponse =
                "{"
                + "\"status\":\"SUCCESS\","
                + "\"operation\":\"INSERT\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"rowsInserted\":"
                + rowsInserted
                + "}";

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-INSERT] STEP 4 - "
                + "JSON Response built"
                + " | queryId=" + queryId
                + " | RowsInserted=" + rowsInserted
            );

            // ----------------------------------------------------
            // STEP 6 - Kafka SUCCESS event
            // ----------------------------------------------------

            String kafkaResponsePayload =
                "{"
                + "\"event\":\"DB2_CAMEL_BAMOE_INSERT_SUCCESS\","
                + "\"operation\":\"INSERT\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"rowsInserted\":"
                + rowsInserted
                + ","
                + "\"timestamp\":\""
                + Instant.now().toString()
                + "\""
                + "}";

            publishToKafka(
                KAFKA_PUBLISH_TOPIC,
                kafkaResponsePayload
            );

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-INSERT] STEP 5 - "
                + "Published SUCCESS event to Kafka"
                + " | Topic=" + KAFKA_PUBLISH_TOPIC
                + " | queryId=" + queryId
                + " | RowsInserted=" + rowsInserted
                + " | DurationMs=" + ms
            );

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-INSERT] ROUTE COMPLETE"
                + " | queryId=" + queryId
                + " | DurationMs=" + ms
            );

            CamelApplication.logOperation(
                "INSERT",
                queryId,
                "SUCCESS",
                "RowsInserted=" + rowsInserted
            );

            return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(jsonResponse);

        } catch (Exception e) {

            String reason =
                cleanErrorMessage(e);

            long ms =
                Instant.now().toEpochMilli()
                - start.toEpochMilli();

            CamelApplication.log(
                "ERROR",
                "[BAMOE-CAMEL-DB2-INSERT] ROUTE FAILED"
                + " | queryId=" + queryId
                + " | Reason=" + reason
                + " | DurationMs=" + ms
            );

            CamelApplication.logOperation(
                "INSERT",
                queryId,
                "ERROR",
                reason
            );

            publishToKafka(
                KAFKA_ERROR_TOPIC,
                "{"
                + "\"event\":\"BAMOE_CAMEL_INSERT_ERROR\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"message\":\""
                + escapeJson(reason)
                + "\","
                + "\"timestamp\":\""
                + Instant.now().toString()
                + "\""
                + "}"
            );

            return ResponseEntity.status(500)
                .contentType(MediaType.APPLICATION_JSON)
                .body(
                    "{"
                    + "\"status\":\"ERROR\","
                    + "\"operation\":\"INSERT\","
                    + "\"queryId\":\""
                    + escapeJson(queryId)
                    + "\","
                    + "\"message\":\""
                    + escapeJson(reason)
                    + "\""
                    + "}"
                );
        }
    }

    // ============================================================
    // UPDATE
    // ============================================================

    @PostMapping("/execute-update")
    public ResponseEntity<String> executeUpdate(
            @RequestBody String requestBody) {

        Instant start = Instant.now();

        String queryId = "UNKNOWN";

        CamelApplication.log(
            "INFO",
            "[BAMOE-CAMEL-DB2-UPDATE] STEP 1 - "
            + "HTTP POST Request Received"
            + " | Body=" + requestBody
        );

        try {

            // ----------------------------------------------------
            // STEP 1 - Parse JSON
            // ----------------------------------------------------

            JsonNode root =
                objectMapper.readTree(requestBody);

            queryId =
                getRequiredText(
                    root,
                    "queryId"
                );

            if (queryId == null
                    || queryId.isBlank()) {

                return buildUpdateErrorResponse(
                    400,
                    "UNKNOWN",
                    "Missing queryId field in request body."
                    + " Available queryIds: "
                    + camelQueryRegistry.getAvailableQueryIds(),
                    start
                );
            }

            JsonNode paramsNode =
                root.get("params");

            if (paramsNode == null
                    || !paramsNode.isObject()) {

                return buildUpdateErrorResponse(
                    400,
                    queryId,
                    "Missing params field in request body.",
                    start
                );
            }

            // ----------------------------------------------------
            // STEP 2 - Get SQL from registry
            // ----------------------------------------------------

            String sqlQuery =
                camelQueryRegistry.getQuery(
                    queryId
                );

            if (sqlQuery == null) {

                return buildUpdateErrorResponse(
                    400,
                    queryId,
                    "Unknown queryId: " + queryId
                    + ". Available queryIds: "
                    + camelQueryRegistry.getAvailableQueryIds(),
                    start
                );
            }

            // ----------------------------------------------------
            // Validate that the SQL is actually UPDATE.
            //
            // This prevents someone from accidentally sending
            // an INSERT/SELECT queryId to /execute-update.
            //
            // This does NOT check for specific query IDs.
            // ----------------------------------------------------

            if (!isUpdateSql(sqlQuery)) {

                return buildUpdateErrorResponse(
                    400,
                    queryId,
                    "The queryId '" + queryId
                    + "' does not reference an UPDATE statement."
                    + " /execute-update can only execute UPDATE SQL.",
                    start
                );
            }

            // ----------------------------------------------------
            // Convert params JSON into a Map.
            //
            // LinkedHashMap preserves JSON order, but the connector
            // will ultimately use SQL column names, so order does
            // not matter.
            // ----------------------------------------------------

            Map<String, Object> params =
                objectMapper.convertValue(
                    paramsNode,
                    new TypeReference<
                        LinkedHashMap<String, Object>
                    >() {}
                );

            if (params.isEmpty()) {

                return buildUpdateErrorResponse(
                    400,
                    queryId,
                    "The params object cannot be empty.",
                    start
                );
            }

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-UPDATE] STEP 1 - Parsed successfully"
                + " | queryId=" + queryId
                + " | ParameterCount=" + params.size()
            );

            // ----------------------------------------------------
            // STEP 3 - Kafka REQUEST event
            // ----------------------------------------------------

            String kafkaRequestPayload =
                "{"
                + "\"event\":\"BAMOE_CAMEL_UPDATE_REQUEST_RECEIVED\","
                + "\"message\":\"Camel has received an UPDATE request.\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"timestamp\":\""
                + Instant.now().toString()
                + "\""
                + "}";

            publishToKafka(
                KAFKA_PUBLISH_TOPIC,
                kafkaRequestPayload
            );

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-UPDATE] STEP 2 - "
                + "Kafka REQUEST event published"
                + " | Topic=" + KAFKA_PUBLISH_TOPIC
                + " | queryId=" + queryId
            );

            // ----------------------------------------------------
            // STEP 4 - Execute UPDATE
            // ----------------------------------------------------

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-UPDATE] STEP 3 - "
                + "Calling CamelDb2Connector"
                + " | queryId=" + queryId
            );

            int rowsUpdated =
                camelDb2Connector.executeUpdate(
                    sqlQuery,
                    params
                );

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-UPDATE] STEP 3 - "
                + "DB2 UPDATE complete"
                + " | queryId=" + queryId
                + " | RowsUpdated=" + rowsUpdated
            );

            // ----------------------------------------------------
            // STEP 5 - Build response
            // ----------------------------------------------------

            long ms =
                Instant.now().toEpochMilli()
                - start.toEpochMilli();

            String jsonResponse =
                "{"
                + "\"status\":\"SUCCESS\","
                + "\"operation\":\"UPDATE\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"rowsUpdated\":"
                + rowsUpdated
                + "}";

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-UPDATE] STEP 4 - "
                + "JSON Response built"
                + " | queryId=" + queryId
                + " | RowsUpdated=" + rowsUpdated
            );

            // ----------------------------------------------------
            // STEP 6 - Kafka SUCCESS event
            // ----------------------------------------------------

            String kafkaResponsePayload =
                "{"
                + "\"event\":\"DB2_CAMEL_BAMOE_UPDATE_SUCCESS\","
                + "\"operation\":\"UPDATE\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"rowsUpdated\":"
                + rowsUpdated
                + ","
                + "\"timestamp\":\""
                + Instant.now().toString()
                + "\""
                + "}";

            publishToKafka(
                KAFKA_PUBLISH_TOPIC,
                kafkaResponsePayload
            );

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-UPDATE] STEP 5 - "
                + "Published SUCCESS event to Kafka"
                + " | Topic=" + KAFKA_PUBLISH_TOPIC
                + " | queryId=" + queryId
                + " | RowsUpdated=" + rowsUpdated
                + " | DurationMs=" + ms
            );

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-UPDATE] ROUTE COMPLETE"
                + " | queryId=" + queryId
                + " | DurationMs=" + ms
            );

            CamelApplication.logOperation(
                "UPDATE",
                queryId,
                "SUCCESS",
                "RowsUpdated=" + rowsUpdated
            );

            return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(jsonResponse);

        } catch (Exception e) {

            String reason =
                cleanErrorMessage(e);

            long ms =
                Instant.now().toEpochMilli()
                - start.toEpochMilli();

            CamelApplication.log(
                "ERROR",
                "[BAMOE-CAMEL-DB2-UPDATE] ROUTE FAILED"
                + " | queryId=" + queryId
                + " | Reason=" + reason
                + " | DurationMs=" + ms
            );

            CamelApplication.logOperation(
                "UPDATE",
                queryId,
                "ERROR",
                reason
            );

            publishToKafka(
                KAFKA_ERROR_TOPIC,
                "{"
                + "\"event\":\"BAMOE_CAMEL_UPDATE_ERROR\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"message\":\""
                + escapeJson(reason)
                + "\","
                + "\"timestamp\":\""
                + Instant.now().toString()
                + "\""
                + "}"
            );

            return ResponseEntity.status(500)
                .contentType(MediaType.APPLICATION_JSON)
                .body(
                    "{"
                    + "\"status\":\"ERROR\","
                    + "\"operation\":\"UPDATE\","
                    + "\"queryId\":\""
                    + escapeJson(queryId)
                    + "\","
                    + "\"message\":\""
                    + escapeJson(reason)
                    + "\""
                    + "}"
                );
        }
    }

    // ============================================================
    // SQL OPERATION VALIDATION
    // ============================================================

    // Determines whether the SQL supplied by the registry
    // is an INSERT statement.
    //
    // This is intentionally based on SQL, NOT queryId.
    private boolean isInsertSql(String sqlQuery) {

        if (sqlQuery == null) {
            return false;
        }

        String normalized =
            sqlQuery.trim()
                .toUpperCase();

        return normalized.startsWith("INSERT");
    }

    // Determines whether the SQL supplied by the registry
    // is an UPDATE statement.
    //
    // This is intentionally based on SQL, NOT queryId.
    private boolean isUpdateSql(String sqlQuery) {

        if (sqlQuery == null) {
            return false;
        }

        String normalized =
            sqlQuery.trim()
                .toUpperCase();

        return normalized.startsWith("UPDATE");
    }

    // ============================================================
    // KAFKA
    // ============================================================

    private void publishToKafka(
            String topic,
            String payload) {

        try {

            producerTemplate.sendBody(
                "kafka:" + topic
                + "?brokers=" + kafkaBootstrapServers
                + "&retries=3"
                + "&requestTimeoutMs=3000"
                + "&deliveryTimeoutMs=5000"
                + "&maxBlockMs=3000",
                payload
            );

        } catch (Exception e) {

            CamelApplication.log(
                "ERROR",
                "[BAMOE-CAMEL-DB2] Failed to publish to Kafka"
                + " | Topic=" + topic
                + " | Reason=" + e.getMessage()
            );
        }
    }

    // ============================================================
    // ERROR RESPONSE
    // ============================================================

    private ResponseEntity<String> buildErrorResponse(
            int status,
            String queryId,
            String caseId,
            String message,
            Instant start) {

        long ms =
            Instant.now().toEpochMilli()
            - start.toEpochMilli();

        CamelApplication.log(
            "ERROR",
            "[BAMOE-CAMEL-DB2] Validation error"
            + " | queryId=" + queryId
            + " | caseId=" + caseId
            + " | Message=" + message
        );

        publishToKafka(
            KAFKA_ERROR_TOPIC,
            "{"
            + "\"event\":\"BAMOE_CAMEL_VALIDATION_ERROR\","
            + "\"queryId\":\""
            + escapeJson(queryId)
            + "\","
            + "\"caseId\":\""
            + escapeJson(caseId)
            + "\","
            + "\"message\":\""
            + escapeJson(message)
            + "\","
            + "\"durationMs\":"
            + ms
            + ","
            + "\"timestamp\":\""
            + Instant.now().toString()
            + "\""
            + "}"
        );

        return ResponseEntity.status(status)
            .contentType(MediaType.APPLICATION_JSON)
            .body(
                "{"
                + "\"status\":\"ERROR\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"caseId\":\""
                + escapeJson(caseId)
                + "\","
                + "\"message\":\""
                + escapeJson(message)
                + "\""
                + "}"
            );
    }

    private ResponseEntity<String> buildInsertErrorResponse(
            int status,
            String queryId,
            String message,
            Instant start) {

        long ms =
            Instant.now().toEpochMilli()
            - start.toEpochMilli();

        CamelApplication.log(
            "ERROR",
            "[BAMOE-CAMEL-DB2-INSERT] Validation error"
            + " | queryId=" + queryId
            + " | Message=" + message
        );

        publishToKafka(
            KAFKA_ERROR_TOPIC,
            "{"
            + "\"event\":\"BAMOE_CAMEL_INSERT_VALIDATION_ERROR\","
            + "\"queryId\":\""
            + escapeJson(queryId)
            + "\","
            + "\"message\":\""
            + escapeJson(message)
            + "\","
            + "\"durationMs\":"
            + ms
            + ","
            + "\"timestamp\":\""
            + Instant.now().toString()
            + "\""
            + "}"
        );

        return ResponseEntity.status(status)
            .contentType(MediaType.APPLICATION_JSON)
            .body(
                "{"
                + "\"status\":\"ERROR\","
                + "\"operation\":\"INSERT\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"message\":\""
                + escapeJson(message)
                + "\""
                + "}"
            );
    }

    private ResponseEntity<String> buildUpdateErrorResponse(
            int status,
            String queryId,
            String message,
            Instant start) {

        long ms =
            Instant.now().toEpochMilli()
            - start.toEpochMilli();

        CamelApplication.log(
            "ERROR",
            "[BAMOE-CAMEL-DB2-UPDATE] Validation error"
            + " | queryId=" + queryId
            + " | Message=" + message
        );

        publishToKafka(
            KAFKA_ERROR_TOPIC,
            "{"
            + "\"event\":\"BAMOE_CAMEL_UPDATE_VALIDATION_ERROR\","
            + "\"queryId\":\""
            + escapeJson(queryId)
            + "\","
            + "\"message\":\""
            + escapeJson(message)
            + "\","
            + "\"durationMs\":"
            + ms
            + ","
            + "\"timestamp\":\""
            + Instant.now().toString()
            + "\""
            + "}"
        );

        return ResponseEntity.status(status)
            .contentType(MediaType.APPLICATION_JSON)
            .body(
                "{"
                + "\"status\":\"ERROR\","
                + "\"operation\":\"UPDATE\","
                + "\"queryId\":\""
                + escapeJson(queryId)
                + "\","
                + "\"message\":\""
                + escapeJson(message)
                + "\""
                + "}"
            );
    }

    // ============================================================
    // JSON HELPERS
    // ============================================================

    private String getRequiredText(
            JsonNode node,
            String fieldName) {

        if (node == null) {
            return null;
        }

        JsonNode value =
            node.get(fieldName);

        if (value == null
                || value.isNull()) {

            return null;
        }

        if (!value.isValueNode()) {
            return null;
        }

        return value.asText();
    }

    private String cleanErrorMessage(
            Exception e) {

        String reason =
            e.getMessage();

        if (reason == null
                || reason.isBlank()) {

            reason =
                e.getClass()
                    .getSimpleName();
        }

        if (reason.contains("\n")) {

            reason =
                reason.substring(
                    0,
                    reason.indexOf("\n")
                );
        }

        return reason;
    }

    private String escapeJson(
            String value) {

        if (value == null) {
            return "";
        }

        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\r", "\\r")
            .replace("\n", "\\n");
    }

    private String toJsonValue(
            Object value) {

        if (value == null) {
            return "null";
        }

        if (value instanceof Number) {
            return value.toString();
        }

        if (value instanceof Boolean) {
            return value.toString();
        }

        return "\""
            + escapeJson(
                value.toString().trim()
            )
            + "\"";
    }
}