package org.apache.samza.tools.client.interfaces;

public class ExecutionContext {
    public MessageFormat m_messageFormat;

    public void setMessageFormat(MessageFormat messageFormat) {
        m_messageFormat = messageFormat;
    }

    public MessageFormat getMessageFormat() {
        return m_messageFormat;
    }

    public enum MessageFormat {
        PRETTY,
        COMPACT
    }
}
