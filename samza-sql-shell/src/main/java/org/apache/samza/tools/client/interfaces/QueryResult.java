package org.apache.samza.tools.client.interfaces;


public class QueryResult {
    private int m_execId;
    private SamzaSqlSchema m_schema;

    public QueryResult(int execId, SamzaSqlSchema schema) {
        if(schema == null)
            throw new IllegalArgumentException();
        m_execId = execId;
        m_schema = schema;
    }

    public int getExecutionId() {
        return m_execId;
    }

    public SamzaSqlSchema getTableSchema() {
        return m_schema;
    }
}
