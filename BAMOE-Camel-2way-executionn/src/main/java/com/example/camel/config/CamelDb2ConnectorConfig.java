package com.example.camel.config;

import com.example.camel.CamelApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.sql.DataSource;

// Registers the DB2 datasource bean used by CamelDb2Connector
// to connect to the DB2 CASE table
@Configuration
@DependsOn("credentialsConfig")
public class CamelDb2ConnectorConfig {

    // DB2 connection properties loaded from application.properties
    @Value("${camel.db2.case.serverName}")          private String  serverName;
    @Value("${camel.db2.case.port}")                private int     port;
    @Value("${camel.db2.case.databaseName}")        private String  databaseName;
    @Value("${camel.db2.case.username}")            private String  username;
    @Value("${camel.db2.case.password}")            private String  password;
    @Value("${camel.db2.case.driverType}")          private int     driverType;
    @Value("${camel.db2.case.securityMechanism}")   private short   securityMechanism;
    @Value("${camel.db2.case.sslConnection}")       private boolean sslConnection;
    @Value("${camel.db2.case.encryptionAlgorithm}") private int     encryptionAlgorithm;
    @Value("${camel.db2.case.sslTrustStoreLocation}") private String sslTrustStoreLocation;
    @Value("${camel.db2.case.sslTrustStorePassword}") private String sslTrustStorePassword;

    // Registers the caseDb2DataSource bean injected into CamelDb2Connector
    @Bean(name = "caseDb2DataSource")
    public DataSource caseDb2DataSource() {
        CamelApplication.log("INFO",
            "[BAMOE-CAMEL-DB2-CONNECTOR-CONFIG] Registering caseDb2DataSource bean"
            + " | Server="   + serverName
            + " | Port="     + port
            + " | Database=" + databaseName
            + " | SSL="      + sslConnection
            + " | TrustStore=" + sslTrustStoreLocation);

        com.ibm.db2.jcc.DB2SimpleDataSource ds = new com.ibm.db2.jcc.DB2SimpleDataSource();
        ds.setServerName(serverName);
        ds.setPortNumber(port);
        ds.setDatabaseName(databaseName);
        ds.setUser(username);
        ds.setPassword(password);
        ds.setDriverType(driverType);
        ds.setSecurityMechanism(securityMechanism);
        ds.setSslConnection(sslConnection);
        ds.setEncryptionAlgorithm(encryptionAlgorithm);
        ds.setRetrieveMessagesFromServerOnGetMessage(true);

        if (sslConnection && sslTrustStoreLocation != null && !sslTrustStoreLocation.isBlank()) {
            ds.setSslTrustStoreLocation(sslTrustStoreLocation);
            ds.setSslTrustStorePassword(sslTrustStorePassword);
        }

        CamelApplication.log("INFO",
            "[BAMOE-CAMEL-DB2-CONNECTOR-CONFIG] caseDb2DataSource bean registered successfully");
        return ds;
    }
}