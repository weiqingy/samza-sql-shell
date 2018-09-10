package org.apache.samza.tools.client.impl;

import java.net.URI;
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

    @Override
    public void start(ExecutionContext context) {

    }

    @Override
    public void stop(ExecutionContext context) {

    }

    @Override
    public List<String> listTables(ExecutionContext context) {
        List<String> tableNames = new ArrayList<String>();
        tableNames.add("kafka.ProfileChangeStream");

        return tableNames;
    }

    @Override
    public TableSchema getTableScema(ExecutionContext context, String tableName) {
        return TableSchemaBuilder.builder().appendColumn("Key", "String")
                .appendColumn("Name", "String")
                .appendColumn("NewCompany", "String")
                .appendColumn("OldCompany", "String")
                .appendColumn("ProfileChangeTimestamp", "Date")
                .toTableSchema();
    }

    @Override
    public QueryResult executeQuery(ExecutionContext context, String statement) {
        TableSchema schema = TableSchemaBuilder.builder().
                appendColumn("All", "String").toTableSchema();

        return new QueryResult(1, schema);
    }

    @Override
    public int getRowCount() {
        return 0;
    }

    @Override
    public List<String[]> retrieveQueryResult(ExecutionContext context, int startRow, int endRow) {
        return null;
    }

    @Override
    public List<String[]> consumeQueryResult(ExecutionContext context, int startRow, int endRow) {
        throw new ExecutionException("not supported");
    }

    @Override
    public NonQueryResult executeNonQuery(ExecutionContext context,  URI sqlFile) {
        return null;
    }

    @Override
    public NonQueryResult executeNonQuery(ExecutionContext context, List<String> statement) {
        return new NonQueryResult(++m_execIdSeq, true);
    }

    @Override
    public boolean stopExecution(ExecutionContext context, int exeId) {
        return false;
    }

    @Override
    public boolean removeExecution(ExecutionContext context, int exeId) {
        return false;
    }

    @Override
    public ExecutionStatus queryExecutionStatus(int execId) {
        return null;
    }

    @Override
    public String getErrorMsg() {
        return null;
    }

    private boolean stopExecution(int execId) {
        return false;
    }
}
