package org.apache.samza.tools.client.cli;

import org.apache.samza.tools.client.interfaces.ExecutionContext;

import java.io.PrintWriter;

public class CliEnvironment {
    private ExecutionContext.MessageFormat m_messageFormat = ExecutionContext.MessageFormat.COMPACT;
    private static final String m_messageFormatEnvVar = "OUTPUT";

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

    /**
     *
     * @param var
     * @param val
     * @return 0 : succeed
     *         -1: invalid var
     *         -2: invalid val
     */
    public int setEnvironmentVariable(String var, String val) {
        switch (var.toUpperCase()) {
            case m_messageFormatEnvVar:
                ExecutionContext.MessageFormat messageFormat = null;
                try {
                    messageFormat = ExecutionContext.MessageFormat.valueOf(val.toUpperCase());
                } catch(IllegalArgumentException e) {
                }
                if(messageFormat == null) {
                    return -2;
                }
                m_messageFormat = messageFormat;
                break;
            default:
                return -1;
        }

        return 0;
    }

    public void printAll(PrintWriter writer) {
        writer.print(m_messageFormatEnvVar);
        writer.print('=');
        writer.println(m_messageFormat.name());
        
        writer.println();
        writer.flush();
    }
}
