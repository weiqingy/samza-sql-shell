package org.apache.samza.tools.client.impl;

import org.apache.samza.tools.client.interfaces.*;

import java.util.ArrayList;
import java.util.List;

/**
 *  For local testing purpose when SamzaSqlExecutor is under construction.
 */

public class FakeExecutor implements SqlExecutor {
    private int m_execIdSeq;
    private String m_lastErrMsg = "";

    public FakeExecutor() {

    }

    public void start(ExecutionContext context) {

    }

    public void stop(ExecutionContext context) {

    }

    public List<String> listTables(ExecutionContext context) {
        List<String> tableNames = new ArrayList<String>();
        tableNames.add("kafka.ProfileChangeStream");

        return tableNames;
    }

    public TableSchema getTableScema(ExecutionContext context, String tableName) {
        return TableSchemaBuilder.builder().appendColumn("Key", "String")
                .appendColumn("Name", "String")
                .appendColumn("NewCompany", "String")
                .appendColumn("OldCompany", "String")
                .appendColumn("ProfileChangeTimestamp", "Date")
                .toTableSchema();
    }


    public QueryResult executeQuery(ExecutionContext context, String statement) {
        TableSchema schema = TableSchemaBuilder.builder().
                appendColumn("All", "String").toTableSchema();

        return new QueryResult(1, schema);
    }

    public int getRowCount() {
        return 0;
    }

    public List<String[]> retrieveQueryResult(ExecutionContext context, int startRow, int endRow) {
        return null;
    }

    public NonQueryResult executeNonQuery(ExecutionContext context, List<String> statement) {
        return new NonQueryResult(++m_execIdSeq, true);
    }

    public boolean stopExecution(ExecutionContext context, int exeId) {
        return false;
    }

    public boolean removeExecution(ExecutionContext context, int exeId) {
        return false;
    }

    public ExecutionStatus queryExecutionStatus(int execId) {
        return null;
    }

    public boolean stopExecution(int execId) {
        return false;
    }

    public String getErrorMsg() {
        return null;
    }
}
