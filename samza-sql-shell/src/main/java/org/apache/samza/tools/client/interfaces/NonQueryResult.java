package org.apache.samza.tools.client.interfaces;

import java.util.List;


public class NonQueryResult {
    private int m_execId;
    private boolean m_success;
    private List<String> m_submittedStmts;
    private List<String> m_nonSubmittedStmts;

    public NonQueryResult(int execId, boolean success) {
        m_execId = execId;
        m_success = success;
    }

    public NonQueryResult(int execId, boolean success, List<String> submittedStmts, List<String> nonSubmittedStmts) {
        m_execId = execId;
        m_success = success;
        m_submittedStmts = submittedStmts;
        m_nonSubmittedStmts = nonSubmittedStmts;
    }

    public int getExecutionId() {
        return m_execId;
    }

    public boolean succeeded() {
        return m_success;
    }

    public List<String> getSubmittedStmts() {
        return m_submittedStmts;
    }

    public List<String> geNonSubmittedStmts() {
        return m_nonSubmittedStmts;
    }
}
