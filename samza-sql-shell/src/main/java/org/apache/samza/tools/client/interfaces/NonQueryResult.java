package org.apache.samza.tools.client.interfaces;


public class NonQueryResult {
    private int m_execId;
    private boolean m_success;

    public NonQueryResult(int execId, boolean success) {
        m_execId = execId;
        m_success = success;
    }

    public int getExecutionId() {
        return m_execId;
    }

    public boolean succeeded() {
        return m_success;
    }
}
