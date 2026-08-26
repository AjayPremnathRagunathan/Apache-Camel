package com.example.camel.connector;

import com.example.camel.CamelApplication;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Dedicated bridge between Camel and IBM DB2.
// Handles SELECT/FETCH, INSERT, and UPDATE operations.
@Component
public class CamelDb2Connector {

    // DB2 datasource injected from CamelDb2ConnectorConfig
    private final DataSource caseDb2DataSource;

    public CamelDb2Connector(
            @Qualifier("caseDb2DataSource") DataSource caseDb2DataSource) {

        this.caseDb2DataSource = caseDb2DataSource;

        CamelApplication.log(
            "INFO",
            "[BAMOE-CAMEL-DB2-CONNECTOR] CamelDb2Connector initialized"
            + " | DataSource=caseDb2DataSource"
        );
    }

    // ============================================================
    // FETCH / SELECT
    // ============================================================

    // Executes a SELECT query using one caseId parameter.
    // Existing FETCH functionality is preserved.
    public List<Map<String, Object>> executeQuery(
            String sqlQuery,
            String caseId) throws CamelDb2ConnectorException {

        Instant queryStart = Instant.now();

        CamelApplication.log(
            "INFO",
            "[BAMOE-CAMEL-DB2-CONNECTOR] Opening DB2 connection"
            + " | caseId=" + caseId
            + " | Query=" + sqlQuery
        );

        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            connection = caseDb2DataSource.getConnection();

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-CONNECTOR] DB2 connection acquired"
                + " | caseId=" + caseId
                + " | AutoCommit=" + connection.getAutoCommit()
            );

            statement = connection.prepareStatement(sqlQuery);

            // Existing FETCH queries use one ? placeholder for caseId.
            statement.setString(1, caseId);

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-CONNECTOR] Executing DB2 query"
                + " | caseId=" + caseId
                + " | SQL=" + sqlQuery.replaceAll("\\s+", " ").trim()
            );

            resultSet = statement.executeQuery();

            List<Map<String, Object>> results =
                mapResultSet(resultSet, caseId);

            long durationMs =
                Instant.now().toEpochMilli() - queryStart.toEpochMilli();

            if (results.isEmpty()) {

                CamelApplication.log(
                    "INFO",
                    "[BAMOE-CAMEL-DB2-CONNECTOR] DB2 query returned no records"
                    + " | caseId=" + caseId
                    + " | DurationMs=" + durationMs
                );

            } else {

                CamelApplication.log(
                    "INFO",
                    "[BAMOE-CAMEL-DB2-CONNECTOR] DB2 query successful"
                    + " | caseId=" + caseId
                    + " | RowsReturned=" + results.size()
                    + " | DurationMs=" + durationMs
                    + " | Columns=" + results.get(0).keySet()
                );
            }

            return results;

        } catch (SQLException sqlEx) {

            long durationMs =
                Instant.now().toEpochMilli() - queryStart.toEpochMilli();

            CamelApplication.log(
                "ERROR",
                buildSqlErrorDetail(sqlEx, caseId, durationMs)
            );

            throw new CamelDb2ConnectorException(
                "DB2 query failed for caseId=" + caseId
                + " | SQLState=" + sqlEx.getSQLState()
                + " | ErrorCode=" + sqlEx.getErrorCode()
                + " | Message=" + sqlEx.getMessage(),
                sqlEx
            );

        } finally {

            closeJdbcResources(
                resultSet,
                statement,
                connection,
                caseId
            );
        }
    }

    // ============================================================
    // INSERT
    // ============================================================

    // Generic INSERT execution.
    //
    // IMPORTANT:
    // - queryId is NOT used here.
    // - The SQL comes from CamelQueryRegistry.
    // - The column names are extracted from the INSERT statement.
    // - Request parameters are matched by column name.
    // - Therefore JSON parameter order does NOT matter.
    //
    // Example:
    //
    // INSERT INTO TABLE
    // (COL1, COL2, COL3)
    // VALUES (?, ?, ?)
    //
    // params:
    // {
    //    "COL2": 100,
    //    "COL1": "ABC",
    //    "COL3": "2026-08-14 17:00:00"
    // }
    //
    // will still bind correctly as:
    // COL1 -> ?
    // COL2 -> ?
    // COL3 -> ?
    public int executeInsert(
            String sqlQuery,
            Map<String, Object> params)
            throws CamelDb2ConnectorException {

        Instant insertStart = Instant.now();

        CamelApplication.log(
            "INFO",
            "[BAMOE-CAMEL-DB2-CONNECTOR] Opening DB2 connection for INSERT"
            + " | SQL=" + sqlQuery.replaceAll("\\s+", " ").trim()
            + " | ParameterCount=" + params.size()
        );

        Connection connection = null;
        PreparedStatement statement = null;

        try {

            connection = caseDb2DataSource.getConnection();

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-CONNECTOR] DB2 connection acquired"
                + " | AutoCommit=" + connection.getAutoCommit()
            );

            // Extract the column names from the INSERT SQL.
            List<String> columnNames =
                extractInsertColumnNames(sqlQuery);

            if (columnNames.isEmpty()) {

                throw new CamelDb2ConnectorException(
                    "Unable to determine INSERT column names from SQL."
                    + " SQL must use the format INSERT INTO table"
                    + " (column1, column2, ...) VALUES (?, ?, ...).",
                    null
                );
            }

            int parameterCount =
                countQuestionMarks(sqlQuery);

            if (parameterCount != columnNames.size()) {

                throw new CamelDb2ConnectorException(
                    "INSERT parameter mismatch."
                    + " SQL contains " + parameterCount
                    + " placeholders but "
                    + columnNames.size()
                    + " column names were found.",
                    null
                );
            }

            if (params.size() != parameterCount) {

                throw new CamelDb2ConnectorException(
                    "INSERT parameter count mismatch."
                    + " SQL expects " + parameterCount
                    + " parameters but request contains "
                    + params.size() + " parameters.",
                    null
                );
            }

            // Normalize incoming parameter names to uppercase.
            Map<String, Object> normalizedParams =
                normalizeParameterNames(params);

            // Validate that every SQL column has a corresponding
            // request parameter.
            for (String columnName : columnNames) {

                if (!normalizedParams.containsKey(
                        columnName.toUpperCase())) {

                    throw new CamelDb2ConnectorException(
                        "Missing INSERT parameter for column: "
                        + columnName,
                        null
                    );
                }
            }

            statement = connection.prepareStatement(sqlQuery);

            // Bind values according to the SQL column order.
            for (int i = 0; i < columnNames.size(); i++) {

                String columnName = columnNames.get(i);

                Object value =
                    normalizedParams.get(
                        columnName.toUpperCase()
                    );

                bindParameter(
                    statement,
                    i + 1,
                    value
                );

                CamelApplication.log(
                    "INFO",
                    "[BAMOE-CAMEL-DB2-CONNECTOR] INSERT parameter bound"
                    + " | Position=" + (i + 1)
                    + " | Column=" + columnName
                    + " | JavaType="
                    + (value == null
                        ? "NULL"
                        : value.getClass().getSimpleName())
                );
            }

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-CONNECTOR] Executing DB2 INSERT"
                + " | SQL=" + sqlQuery.replaceAll("\\s+", " ").trim()
            );

            int rowsInserted = statement.executeUpdate();

            long durationMs =
                Instant.now().toEpochMilli()
                - insertStart.toEpochMilli();

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-CONNECTOR] DB2 INSERT successful"
                + " | RowsInserted=" + rowsInserted
                + " | DurationMs=" + durationMs
            );

            return rowsInserted;

        } catch (SQLException sqlEx) {

            long durationMs =
                Instant.now().toEpochMilli()
                - insertStart.toEpochMilli();

            CamelApplication.log(
                "ERROR",
                buildSqlErrorDetail(
                    sqlEx,
                    "INSERT",
                    durationMs
                )
            );

            throw new CamelDb2ConnectorException(
                "DB2 INSERT failed"
                + " | SQLState=" + sqlEx.getSQLState()
                + " | ErrorCode=" + sqlEx.getErrorCode()
                + " | Message=" + sqlEx.getMessage(),
                sqlEx
            );

        } finally {

            closeJdbcResources(
                null,
                statement,
                connection,
                "INSERT"
            );
        }
    }

    // ============================================================
    // UPDATE
    // ============================================================

    // Generic UPDATE execution.
    //
    // IMPORTANT:
    // - queryId is NOT used here.
    // - The SQL comes from CamelQueryRegistry.
    // - Column names are extracted from every "column = ?" pattern
    //   found in the SQL, in the order they appear (covers both
    //   the SET clause and the WHERE clause).
    // - Request parameters are matched by column name.
    // - Therefore JSON parameter order does NOT matter.
    //
    // Example:
    //
    // UPDATE TABLE SET COL1 = ?, COL2 = ? WHERE COL3 = ?
    //
    // params:
    // {
    //    "COL3": 100,
    //    "COL1": "ABC",
    //    "COL2": "XYZ"
    // }
    //
    // will still bind correctly as:
    // COL1 -> ? (1st)
    // COL2 -> ? (2nd)
    // COL3 -> ? (3rd)
    public int executeUpdate(
            String sqlQuery,
            Map<String, Object> params)
            throws CamelDb2ConnectorException {

        Instant updateStart = Instant.now();

        CamelApplication.log(
            "INFO",
            "[BAMOE-CAMEL-DB2-CONNECTOR] Opening DB2 connection for UPDATE"
            + " | SQL=" + sqlQuery.replaceAll("\\s+", " ").trim()
            + " | ParameterCount=" + params.size()
        );

        Connection connection = null;
        PreparedStatement statement = null;

        try {

            connection = caseDb2DataSource.getConnection();

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-CONNECTOR] DB2 connection acquired"
                + " | AutoCommit=" + connection.getAutoCommit()
            );

            // Extract the column names from the UPDATE SQL,
            // in the order the ? placeholders appear.
            List<String> columnNames =
                extractUpdateColumnNames(sqlQuery);

            if (columnNames.isEmpty()) {

                throw new CamelDb2ConnectorException(
                    "Unable to determine UPDATE column names from SQL."
                    + " SQL must use the format UPDATE table SET"
                    + " column1 = ?, column2 = ? WHERE column3 = ?.",
                    null
                );
            }

            int parameterCount =
                countQuestionMarks(sqlQuery);

            if (parameterCount != columnNames.size()) {

                throw new CamelDb2ConnectorException(
                    "UPDATE parameter mismatch."
                    + " SQL contains " + parameterCount
                    + " placeholders but "
                    + columnNames.size()
                    + " column names were found.",
                    null
                );
            }

            if (params.size() != parameterCount) {

                throw new CamelDb2ConnectorException(
                    "UPDATE parameter count mismatch."
                    + " SQL expects " + parameterCount
                    + " parameters but request contains "
                    + params.size() + " parameters.",
                    null
                );
            }

            // Normalize incoming parameter names to uppercase.
            Map<String, Object> normalizedParams =
                normalizeParameterNames(params);

            // Validate that every SQL column has a corresponding
            // request parameter.
            for (String columnName : columnNames) {

                if (!normalizedParams.containsKey(
                        columnName.toUpperCase())) {

                    throw new CamelDb2ConnectorException(
                        "Missing UPDATE parameter for column: "
                        + columnName,
                        null
                    );
                }
            }

            statement = connection.prepareStatement(sqlQuery);

            // Bind values according to the SQL placeholder order.
            for (int i = 0; i < columnNames.size(); i++) {

                String columnName = columnNames.get(i);

                Object value =
                    normalizedParams.get(
                        columnName.toUpperCase()
                    );

                bindParameter(
                    statement,
                    i + 1,
                    value
                );

                CamelApplication.log(
                    "INFO",
                    "[BAMOE-CAMEL-DB2-CONNECTOR] UPDATE parameter bound"
                    + " | Position=" + (i + 1)
                    + " | Column=" + columnName
                    + " | JavaType="
                    + (value == null
                        ? "NULL"
                        : value.getClass().getSimpleName())
                );
            }

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-CONNECTOR] Executing DB2 UPDATE"
                + " | SQL=" + sqlQuery.replaceAll("\\s+", " ").trim()
            );

            int rowsUpdated = statement.executeUpdate();

            long durationMs =
                Instant.now().toEpochMilli()
                - updateStart.toEpochMilli();

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-CONNECTOR] DB2 UPDATE successful"
                + " | RowsUpdated=" + rowsUpdated
                + " | DurationMs=" + durationMs
            );

            return rowsUpdated;

        } catch (SQLException sqlEx) {

            long durationMs =
                Instant.now().toEpochMilli()
                - updateStart.toEpochMilli();

            CamelApplication.log(
                "ERROR",
                buildSqlErrorDetail(
                    sqlEx,
                    "UPDATE",
                    durationMs
                )
            );

            throw new CamelDb2ConnectorException(
                "DB2 UPDATE failed"
                + " | SQLState=" + sqlEx.getSQLState()
                + " | ErrorCode=" + sqlEx.getErrorCode()
                + " | Message=" + sqlEx.getMessage(),
                sqlEx
            );

        } finally {

            closeJdbcResources(
                null,
                statement,
                connection,
                "UPDATE"
            );
        }
    }

    // ============================================================
    // PARAMETER BINDING
    // ============================================================

    // Binds JSON values to JDBC parameters using appropriate types.
    //
    // Handles:
    // Integer
    // Long
    // Double / Float
    // BigDecimal
    // Boolean
    // Timestamp strings
    // Date strings
    // Regular strings
    // null
    private void bindParameter(
            PreparedStatement statement,
            int position,
            Object value) throws SQLException {

        if (value == null) {

            statement.setNull(position, Types.NULL);
            return;
        }

        if (value instanceof Integer) {

            statement.setInt(
                position,
                (Integer) value
            );
            return;
        }

        if (value instanceof Long) {

            statement.setLong(
                position,
                (Long) value
            );
            return;
        }

        if (value instanceof Short) {

            statement.setShort(
                position,
                (Short) value
            );
            return;
        }

        if (value instanceof Byte) {

            statement.setByte(
                position,
                (Byte) value
            );
            return;
        }

        if (value instanceof BigDecimal) {

            statement.setBigDecimal(
                position,
                (BigDecimal) value
            );
            return;
        }

        if (value instanceof Double
                || value instanceof Float) {

            // Convert through String to avoid unnecessary
            // floating-point precision issues.
            BigDecimal decimal =
                new BigDecimal(value.toString());

            statement.setBigDecimal(
                position,
                decimal
            );
            return;
        }

        if (value instanceof Boolean) {

            statement.setBoolean(
                position,
                (Boolean) value
            );
            return;
        }

        if (value instanceof Timestamp) {

            statement.setTimestamp(
                position,
                (Timestamp) value
            );
            return;
        }

        if (value instanceof Date) {

            statement.setDate(
                position,
                (Date) value
            );
            return;
        }

        // JSON strings arrive here.
        if (value instanceof String) {

            String stringValue =
                ((String) value).trim();

            // Timestamp:
            // 2026-08-14 17:00:00
            if (stringValue.matches(
                    "\\d{4}-\\d{2}-\\d{2} "
                    + "\\d{2}:\\d{2}:\\d{2}"
                )) {

                statement.setTimestamp(
                    position,
                    Timestamp.valueOf(stringValue)
                );

                return;
            }

            // ISO LocalDateTime:
            // 2026-08-14T17:00:00
            if (stringValue.matches(
                    "\\d{4}-\\d{2}-\\d{2}T"
                    + "\\d{2}:\\d{2}:\\d{2}"
                )) {

                statement.setTimestamp(
                    position,
                    Timestamp.valueOf(
                        LocalDateTime.parse(stringValue)
                    )
                );

                return;
            }

            // Date:
            // 2026-08-14
            if (stringValue.matches(
                    "\\d{4}-\\d{2}-\\d{2}"
                )) {

                statement.setDate(
                    position,
                    Date.valueOf(
                        LocalDate.parse(stringValue)
                    )
                );

                return;
            }

            // Regular VARCHAR / CHAR value.
            statement.setString(
                position,
                stringValue
            );

            return;
        }

        // Final fallback for any other Java type.
        statement.setObject(
            position,
            value
        );
    }

    // ============================================================
    // INSERT SQL HELPERS
    // ============================================================

    // Extracts column names from:
    //
    // INSERT INTO schema.table (COL1, COL2, COL3)
    // VALUES (?, ?, ?)
    private List<String> extractInsertColumnNames(
            String sqlQuery) {

        List<String> columns = new ArrayList<>();

        if (sqlQuery == null || sqlQuery.isBlank()) {
            return columns;
        }

        String normalized =
            sqlQuery.trim();

        int valuesIndex =
            normalized.toUpperCase()
                .indexOf("VALUES");

        if (valuesIndex < 0) {
            return columns;
        }

        String beforeValues =
            normalized.substring(0, valuesIndex);

        int openParen =
            beforeValues.indexOf("(");

        int closeParen =
            beforeValues.lastIndexOf(")");

        if (openParen < 0
                || closeParen < openParen) {

            return columns;
        }

        String columnBlock =
            beforeValues.substring(
                openParen + 1,
                closeParen
            );

        for (String column : columnBlock.split(",")) {

            String cleaned =
                column.trim();

            if (!cleaned.isEmpty()) {
                columns.add(cleaned);
            }
        }

        return columns;
    }

    // ============================================================
    // UPDATE SQL HELPERS
    // ============================================================

    // Extracts column names from an UPDATE statement by finding
    // every "columnName = ?" pattern in order of appearance.
    // This naturally covers both the SET clause and the WHERE
    // clause, since both use the same "column = ?" syntax.
    //
    // UPDATE schema.table SET COL1 = ?, COL2 = ? WHERE COL3 = ?
    //
    // returns: [COL1, COL2, COL3]
    private List<String> extractUpdateColumnNames(
            String sqlQuery) {

        List<String> columns = new ArrayList<>();

        if (sqlQuery == null || sqlQuery.isBlank()) {
            return columns;
        }

        Pattern pattern =
            Pattern.compile(
                "([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*\\?"
            );

        Matcher matcher =
            pattern.matcher(sqlQuery);

        while (matcher.find()) {
            columns.add(matcher.group(1));
        }

        return columns;
    }

    // Counts ? placeholders in the SQL.
    private int countQuestionMarks(
            String sqlQuery) {

        int count = 0;

        if (sqlQuery == null) {
            return count;
        }

        for (char c : sqlQuery.toCharArray()) {

            if (c == '?') {
                count++;
            }
        }

        return count;
    }

    // Converts parameter names to uppercase so:
    //
    // "ROW_CREAT_USER"
    // "row_creat_user"
    //
    // can both be matched to the DB2 column name.
    private Map<String, Object> normalizeParameterNames(
            Map<String, Object> params) {

        Map<String, Object> normalized =
            new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry
                : params.entrySet()) {

            if (entry.getKey() != null) {

                normalized.put(
                    entry.getKey()
                        .trim()
                        .toUpperCase(),
                    entry.getValue()
                );
            }
        }

        return normalized;
    }

    // ============================================================
    // RESULT SET
    // ============================================================

    private List<Map<String, Object>> mapResultSet(
            ResultSet resultSet,
            String caseId) throws SQLException {

        List<Map<String, Object>> rows =
            new ArrayList<>();

        ResultSetMetaData metaData =
            resultSet.getMetaData();

        int colCount =
            metaData.getColumnCount();

        while (resultSet.next()) {

            Map<String, Object> row =
                new LinkedHashMap<>();

            for (int col = 1;
                 col <= colCount;
                 col++) {

                row.put(
                    metaData
                        .getColumnName(col)
                        .toUpperCase(),
                    resultSet.getObject(col)
                );
            }

            CamelApplication.log(
                "INFO",
                "[BAMOE-CAMEL-DB2-CONNECTOR] Row mapped"
                + " | caseId=" + caseId
                + " | Row=" + row
            );

            rows.add(row);
        }

        return rows;
    }

    // ============================================================
    // ERROR HANDLING
    // ============================================================

    private String buildSqlErrorDetail(
            SQLException sqlEx,
            String operation,
            long durationMs) {

        StringBuilder sb =
            new StringBuilder();

        sb.append(
            "[BAMOE-CAMEL-DB2-CONNECTOR] "
            + "DB2 SQL EXCEPTION"
        );

        sb.append(" | Operation=")
            .append(operation);

        sb.append(" | SQLState=")
            .append(sqlEx.getSQLState());

        sb.append(" | ErrorCode=")
            .append(sqlEx.getErrorCode());

        sb.append(" | DurationMs=")
            .append(durationMs);

        String msg =
            sqlEx.getMessage();

        if (msg != null
                && msg.contains("\n")) {

            msg =
                msg.substring(
                    0,
                    msg.indexOf("\n")
                );
        }

        sb.append(" | Message=")
            .append(msg);

        SQLException next =
            sqlEx.getNextException();

        int chainIndex = 1;

        while (next != null) {

            sb.append(
                " | Chained[" +
                chainIndex +
                "]"
            );

            sb.append(
                "=SQLState:"
            ).append(
                next.getSQLState()
            );

            sb.append(
                ",Code:"
            ).append(
                next.getErrorCode()
            );

            next =
                next.getNextException();

            chainIndex++;
        }

        return sb.toString();
    }

    // ============================================================
    // RESOURCE CLEANUP
    // ============================================================

    private void closeJdbcResources(
            ResultSet resultSet,
            PreparedStatement statement,
            Connection connection,
            String operation) {

        try {

            if (resultSet != null) {
                resultSet.close();
            }

        } catch (SQLException e) {

            CamelApplication.log(
                "ERROR",
                "[BAMOE-CAMEL-DB2-CONNECTOR]"
                + " Failed to close ResultSet"
                + " | Operation=" + operation
                + " | Reason=" + e.getMessage()
            );
        }

        try {

            if (statement != null) {
                statement.close();
            }

        } catch (SQLException e) {

            CamelApplication.log(
                "ERROR",
                "[BAMOE-CAMEL-DB2-CONNECTOR]"
                + " Failed to close PreparedStatement"
                + " | Operation=" + operation
                + " | Reason=" + e.getMessage()
            );
        }

        try {

            if (connection != null) {

                connection.close();

                CamelApplication.log(
                    "INFO",
                    "[BAMOE-CAMEL-DB2-CONNECTOR]"
                    + " DB2 connection closed"
                    + " | Operation=" + operation
                );
            }

        } catch (SQLException e) {

            CamelApplication.log(
                "ERROR",
                "[BAMOE-CAMEL-DB2-CONNECTOR]"
                + " Failed to close DB2 Connection"
                + " | Operation=" + operation
                + " | Reason=" + e.getMessage()
            );
        }
    }

    // Typed exception thrown when a DB2 error occurs.
    public static class CamelDb2ConnectorException
            extends Exception {

        public CamelDb2ConnectorException(
                String message,
                Throwable cause) {

            super(message, cause);
        }
    }
}