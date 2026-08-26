package com.example.camel.config;

import com.example.camel.CamelApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.io.support.ResourcePropertySource;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.File;

@Configuration
public class CredentialsConfig {

    @Autowired
    private ConfigurableEnvironment environment;

    // Paths to look for the credentials file
    // Container path first, then local Windows path as fallback
    private static final String[] CREDENTIAL_FILE_PATHS = {
        "/app/db2-credentials.properties",
        "C:/bamoe-dev/db2-credentials.properties"
    };

    @PostConstruct
    public void loadCredentials() {
        for (String path : CREDENTIAL_FILE_PATHS) {
            File file = new File(path);
            if (file.exists()) {
                try {
                    MutablePropertySources sources = environment.getPropertySources();
                    sources.addFirst(new ResourcePropertySource(
                        "db2-credentials", "file:" + path));
                    CamelApplication.log("INFO",
                        "[CREDENTIALS-CONFIG] DB2 credentials loaded from file: " + path);
                    return;
                } catch (Exception e) {
                    CamelApplication.log("ERROR",
                        "[CREDENTIALS-CONFIG] Failed to load credentials from: "
                        + path + " | Reason=" + e.getMessage());
                }
            }
        }
        CamelApplication.log("INFO",
            "[CREDENTIALS-CONFIG] No credentials file found."
            + " Falling back to environment variables DB2_USERNAME / DB2_PASSWORD");
    }
}