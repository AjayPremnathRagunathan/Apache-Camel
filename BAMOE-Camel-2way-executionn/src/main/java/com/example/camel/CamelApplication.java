package com.example.camel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Security;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

// Spring Boot entry point for the BAMOE Camel DB2 Kafka Integration
// scanBasePackages ensures Spring scans all packages including controller
@SpringBootApplication(scanBasePackages = "com.example.camel")
public class CamelApplication {

    // ----------------------------------------------------------
    // Log directory and formats.
    // ----------------------------------------------------------
    static final String LOG_DIR = "logs";

    static final DateTimeFormatter TIMESTAMP_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    static final DateTimeFormatter DATE_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // ----------------------------------------------------------
    // TECHNICAL LOG (troubleshooting / engineering use)
    //
    // Console always shows every line.
    // File only records lines matching one of these keywords,
    // written to a new file each day:
    //     logs/camel-db2-kafka-YYYY-MM-DD.log
    // ----------------------------------------------------------
    private static final String[] FILE_LOG_MARKERS = {
        "ROUTE COMPLETE",
        "ROUTE FAILED",
        "SUCCESSFUL",
        "VALIDATION ERROR",
        "SQL EXCEPTION",
        "APP STARTUP",
        "APP READY",
        "CONNECTOR INITIALIZED"
    };

    // Writes a technical log entry to the console always, and to
    // the daily technical log file when it matches a marker above
    // or is an ERROR.
    public static void log(String level, String message) {

        String line =
            LocalDateTime.now().format(TIMESTAMP_FMT)
            + " | " + level
            + " | " + message;

        // Console always gets the full detailed stream.
        System.out.println(line);

        if (!shouldWriteTechnicalLog(level, message)) {
            return;
        }

        writeLine(technicalLogFile(), line);
    }

    private static boolean shouldWriteTechnicalLog(
            String level,
            String message) {

        if ("ERROR".equalsIgnoreCase(level)) {
            return true;
        }

        if (message == null) {
            return false;
        }

        String upperMessage = message.toUpperCase();

        for (String marker : FILE_LOG_MARKERS) {

            if (upperMessage.contains(marker)) {
                return true;
            }
        }

        return false;
    }

    private static String technicalLogFile() {

        return LOG_DIR + "/camel-db2-kafka-"
            + LocalDate.now().format(DATE_FMT)
            + ".log";
    }

    // ----------------------------------------------------------
    // OPERATIONS LOG (user / UI-facing summary)
    //
    // One clean line per completed operation:
    //     TIMESTAMP | OPERATION | QUERYID | STATUS | DETAILS
    //
    // Written to a new file each day:
    //     logs/camel-operations-YYYY-MM-DD.log
    //
    // This is the file intended to be surfaced in the UI later.
    // It does NOT include internal step-by-step noise - only the
    // final result of each operation the system performed.
    // ----------------------------------------------------------

    // operation: "SELECT" / "INSERT" / "UPDATE"
    // queryId:   the queryId that was executed
    // status:    "SUCCESS" or "ERROR"
    // details:   short human-readable result, e.g.
    //            "RowsInserted=1" or the error message
    public static void logOperation(
            String operation,
            String queryId,
            String status,
            String details) {

        String line =
            LocalDateTime.now().format(TIMESTAMP_FMT)
            + " | " + pad(operation, 6)
            + " | " + pad(queryId, 40)
            + " | " + pad(status, 7)
            + " | " + details;

        // Also echo to console so it's visible during local runs.
        System.out.println(line);

        writeLine(operationsLogFile(), line);
    }

    private static String operationsLogFile() {

        return LOG_DIR + "/camel-operations-"
            + LocalDate.now().format(DATE_FMT)
            + ".log";
    }

    // Right-pads a value to a fixed width so columns line up
    // when the file is viewed as plain text. Values longer than
    // the width are left as-is (not truncated).
    private static String pad(String value, int width) {

        String safeValue =
            (value == null) ? "" : value;

        if (safeValue.length() >= width) {
            return safeValue;
        }

        StringBuilder sb = new StringBuilder(safeValue);

        while (sb.length() < width) {
            sb.append(" ");
        }

        return sb.toString();
    }

    // ----------------------------------------------------------
    // SHARED FILE WRITER
    // ----------------------------------------------------------
    private static void writeLine(String filePath, String line) {

        try {
            Files.createDirectories(Paths.get(LOG_DIR));

            try (PrintWriter pw =
                    new PrintWriter(new FileWriter(filePath, true))) {

                pw.println(line);
            }

        } catch (IOException e) {
            System.out.println(
                "WARNING: Could not write to log file "
                + filePath + " - " + e.getMessage()
            );
        }
    }

    // App entry point - registers Bouncy Castle for DB2 security then starts Spring Boot
    public static void main(String[] args) {

        Security.insertProviderAt(new BouncyCastleProvider(), 1);

        log("INFO", "[APP STARTUP] Bouncy Castle provider inserted");

        ConfigurableApplicationContext ctx =
            SpringApplication.run(CamelApplication.class, args);

        if (ctx.isRunning()) {

            log("INFO", "[APP READY] Application started - BAMOE Camel route active");

            log("INFO", "[APP READY] Listening for BAMOE requests on:"
                + " POST http://vmdevtestrhelaro01.dcss.gov:8080/bamoe/execute-query");
        }
    }
}