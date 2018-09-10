package org.apache.samza.tools.client.interfaces;

public class ExecutionContext {
    private MessageFormat m_messageFormat = MessageFormat.PRETTY;

    public void setMessageFormat(MessageFormat messageFormat) {
        m_messageFormat = messageFormat;
    }

    public MessageFormat getMessageFormat() {
        return m_messageFormat;
    }

    public static enum MessageFormat {
        PRETTY,
        COMPACT
    }
}
