package org.apache.samza.tools.client.interfaces;


public class QueryResult {
    private int m_execId;
    private SamzaSqlSchema m_schema;

    public QueryResult(int execId) {
        m_execId = execId;
    }

    public int getExecutionId() {
        return m_execId;
    }
}
