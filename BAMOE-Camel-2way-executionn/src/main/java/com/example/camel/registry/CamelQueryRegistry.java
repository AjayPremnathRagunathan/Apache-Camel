package com.example.camel.registry;
import com.example.camel.CamelApplication;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
// Central registry that loads all SQL queries from query-registry.properties
// Each query is mapped to a unique queryId sent by BAMOE
// To add new queries simply add them to query-registry.properties
// No Java code changes needed when adding new queries
@Component
public class CamelQueryRegistry {
    // Properties file that stores all SQL queries
    private static final String QUERY_PROPERTIES_FILE = "query-registry.properties";
    // Map of queryId names to their corresponding SQL queries
    // loaded from query-registry.properties on startup
    private Map<String, String> queryMap;
    // Loads the SQL queries from query-registry.properties when the bean is initialized
    // runs automatically after Spring creates the CamelQueryRegistry bean
    @PostConstruct
    public void loadQueries() {
        Map<String, String> map = new HashMap<>();
        try (InputStream inputStream =
                 getClass().getClassLoader().getResourceAsStream(QUERY_PROPERTIES_FILE)) {
            if (inputStream == null) {
                CamelApplication.log("ERROR",
                    "[BAMOE-CAMEL-QUERY-REGISTRY] Properties file not found: "
                    + QUERY_PROPERTIES_FILE);
                queryMap = Collections.unmodifiableMap(map);
                return;
            }
            // Load all key-value pairs from the properties file
            // each key is a queryId and each value is the SQL query
            Properties props = new Properties();
            props.load(inputStream);
            for (String key : props.stringPropertyNames()) {
                // Skip comment lines and empty values
                String sql = props.getProperty(key);
                if (sql != null && !sql.isBlank()) {
                    map.put(key.trim(), sql.trim());
                    CamelApplication.log("INFO",
                        "[BAMOE-CAMEL-QUERY-REGISTRY] Query loaded"
                        + " | QueryId=" + key.trim());
                }
            }
            queryMap = Collections.unmodifiableMap(map);
            CamelApplication.log("INFO",
                "[BAMOE-CAMEL-QUERY-REGISTRY] Query registry initialized"
                + " | Source="            + QUERY_PROPERTIES_FILE
                + " | AvailableQueryIds=" + queryMap.keySet());
        } catch (IOException e) {
            CamelApplication.log("ERROR",
                "[BAMOE-CAMEL-QUERY-REGISTRY] Failed to load queries from "
                + QUERY_PROPERTIES_FILE
                + " | Reason=" + e.getMessage());
            queryMap = Collections.unmodifiableMap(map);
        }
    }
    // Returns the SQL query for the given queryId
    // returns null if the queryId is not found in the registry
    public String getQuery(String queryId) {
        if (queryId == null || queryId.isBlank()) {
            return null;
        }
        return queryMap.get(queryId.trim());
    }
    // Returns all available queryId names
    // used to show available options in error messages
    public Set<String> getAvailableQueryIds() {
        return queryMap.keySet();
    }
    // Checks if a queryId exists in the registry
    public boolean hasQueryId(String queryId) {
        if (queryId == null || queryId.isBlank()) {
            return false;
        }
        return queryMap.containsKey(queryId.trim());
    }
}