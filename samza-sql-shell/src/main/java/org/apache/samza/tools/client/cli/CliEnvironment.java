package org.apache.samza.tools.client.cli;

import org.apache.samza.tools.client.interfaces.ExecutionContext;

public class CliEnvironment {
    private ExecutionContext.MessageFormat m_messageFormat = ExecutionContext.MessageFormat.COMPACT;


    public ExecutionContext.MessageFormat getMessageFormat() {
        return m_messageFormat;
    }

    public void setMessageFormat(ExecutionContext.MessageFormat messageFormat) {
        m_messageFormat = messageFormat;
    }


    public ExecutionContext generateExecutionContext() {
        ExecutionContext exeCtxt = new ExecutionContext();
        exeCtxt.setMessageFormat(m_messageFormat);
        return exeCtxt;
    }
}
