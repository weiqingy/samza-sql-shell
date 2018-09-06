package org.apache.samza.tools.client.interfaces;


public class QueryResult {
    private int m_execId;
    private TableSchema m_schema;

    public QueryResult(int execId, TableSchema schema) {
        if(schema == null)
            throw new IllegalArgumentException();

        m_execId = execId;
        m_schema = schema;
    }

    public int getExecutionId() {
        return m_execId;
    }

    public TableSchema getTableSchema() {
        return m_schema;
    }

}
