package org.apache.samza.tools.client.interfaces;


public class QueryResult {
    private int m_execId;
    private boolean m_success;
    private SqlSchema m_schema;

    public QueryResult(int execId, SqlSchema schema, Boolean success) {
        if(schema == null)
            throw new IllegalArgumentException();
        m_execId = execId;
        m_schema = schema;
        m_success = success;
    }

    public int getExecutionId() {
        return m_execId;
    }

    public SqlSchema getSchema() {
        return m_schema;
    }

    public boolean succeeded() {
        return m_success;
    }
}
