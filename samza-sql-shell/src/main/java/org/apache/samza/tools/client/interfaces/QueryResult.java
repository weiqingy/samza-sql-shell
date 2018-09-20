package org.apache.samza.tools.client.interfaces;


public class QueryResult {
    private int m_execId;
    private SqlSchema m_schema;

    public QueryResult(int execId, SqlSchema schema) {
        if(schema == null)
            throw new IllegalArgumentException();
        m_execId = execId;
        m_schema = schema;
    }

    public int getExecutionId() {
        return m_execId;
    }

    public SqlSchema getSchema() {
        return m_schema;
    }
}
