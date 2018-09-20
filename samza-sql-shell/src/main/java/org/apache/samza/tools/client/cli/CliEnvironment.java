package org.apache.samza.tools.client.cli;

import org.apache.samza.tools.client.interfaces.ExecutionContext;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class CliEnvironment {
    private String m_defaultPersistenceLocation;
    private PrintStream m_stdout = System.out;
    private PrintStream m_stderr = System.err;

    private ExecutionContext.MessageFormat m_messageFormat = ExecutionContext.MessageFormat.COMPACT;
    private static final String m_messageFormatEnvVar = "OUTPUT";

    private Boolean m_debug = false;
    private static final String m_debugEnvVar = "DEBUG";


    public CliEnvironment() {
        m_defaultPersistenceLocation = System.getProperty("user.home");
        if(m_defaultPersistenceLocation == null || m_defaultPersistenceLocation.isEmpty()) {
            m_defaultPersistenceLocation = System.getProperty("user.dir");
        }

        // We control terminal directly; Forbid any Java System.out and System.err stuff so
        // any underlying output will not mess up the console
        if(!m_debug)
            disableJavaSystemOutAndErr();
    }


    public ExecutionContext.MessageFormat getMessageFormat() {
        return m_messageFormat;
    }

    public void setMessageFormat(ExecutionContext.MessageFormat messageFormat) {
        m_messageFormat = messageFormat;
    }

    public boolean isDebug() {
        return m_debug;
    }

    public void setDebug(Boolean debug) {
        m_debug = debug;
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
            case m_debugEnvVar:
                val = val.toLowerCase();
                if(val.equals("true")) {
                    m_debug = true;
                    enableJavaSystemOutAndErr();
                }
                else if(val.equals("false")) {
                    m_debug = false;
                    disableJavaSystemOutAndErr();
                }
                else
                    return -2;
                break;
            default:
                return -1;
        }

        return 0;
    }

    public List<String> getPossibleValues(String var) {
        List<String> vals = new ArrayList<>();
        switch (var.toUpperCase()) {
            case m_messageFormatEnvVar:
                for(ExecutionContext.MessageFormat fm : ExecutionContext.MessageFormat.values()) {
                    vals.add(fm.toString());
                }
                return vals;
            case m_debugEnvVar:
                vals.add("true");
                vals.add("false");
                return vals;
            default:
                return null;
        }
    }

    public void printAll(PrintWriter writer) {
        writer.print(m_messageFormatEnvVar);
        writer.print('=');
        writer.println(m_messageFormat.name());

        writer.print(m_debugEnvVar);
        writer.print('=');
        writer.println(m_debug);

        writer.println();
        writer.flush();
    }

    private void disableJavaSystemOutAndErr() {
        PrintStream ps = new PrintStream(new NullOutputStream());
        System.setOut(ps);
        System.setErr(ps);

        System.out.println("System.out.println");
        System.err.println("System.err.println");
    }

    private void enableJavaSystemOutAndErr() {
        System.setOut(m_stdout);
        System.setErr(m_stderr);

        System.out.println("System.out.println");
        System.err.println("System.err.println");
    }

    private class NullOutputStream extends OutputStream {
        public void close() {}
        public void flush() {}
        public void write(byte[] b) {}
        public void write(byte[] b, int off, int len) {}
        public void write(int b) {}
    }
}
