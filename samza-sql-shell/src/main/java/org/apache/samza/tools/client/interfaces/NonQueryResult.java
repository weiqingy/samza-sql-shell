package org.apache.samza.tools.client.interfaces;

import java.util.List;


public class NonQueryResult {
    private int m_execId;
    private boolean m_success;
    private List<String> m_executedStmts;

    public NonQueryResult(int execId, boolean success) {
        m_execId = execId;
        m_success = success;
    }

    public NonQueryResult(int execId, boolean success, List<String> executedStmts) {
        m_execId = execId;
        m_success = success;
        m_executedStmts = executedStmts;
    }


    public int getExecutionId() {
        return m_execId;
    }

    public boolean succeeded() {
        return m_success;
    }

    public List<String> getExecutedStmts() {
        return m_executedStmts;
    }
}
