package org.apache.samza.tools.client.cli;

import org.apache.samza.tools.client.interfaces.ExecutionContext;
import org.apache.samza.tools.client.interfaces.QueryResult;
import org.apache.samza.tools.client.interfaces.SqlExecutor;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.utils.InfoCmp;

import java.time.LocalTime;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

// TODO: Handle Window Resize
// TODO: Handle signals
// TODO: The UI thread is responsible for fetching data and reresh the UI. We may need
// TODO:    a background thread to fetch the data if we allow users to refresh manually



public class QueryResultExpendedLogView implements CliView {
    private static final int DEFAULT_REFRESH_INTERVAL = 100; // all intervals are in ms
    private int m_refreshInterval = DEFAULT_REFRESH_INTERVAL;
    private LocalTime m_lastRefreshTime;

    private CliShell m_shell;
    private QueryResult m_queryResult;

    private Terminal  m_terminal;
    private SqlExecutor m_executor;
    private ExecutionContext m_exeContext;

    private volatile boolean m_keepRunning = true;

    public QueryResultExpendedLogView() {

    }

    // -- implementation of CliView -------------------------------------------

    public void open(CliShell shell, QueryResult queryResult) {
        m_shell = shell;
        m_queryResult = queryResult;

        m_terminal = shell.getTerminal();
        m_executor = shell.getExecutor();
        m_exeContext = shell.getExecutionContext();

        display();

        while(m_keepRunning) {
            try {
                refresh();

                Thread.sleep(m_refreshInterval);
            } catch (InterruptedException e) {
                continue;
            }
        }
        m_terminal.puts(InfoCmp.Capability.exit_ca_mode);
        m_terminal.puts(InfoCmp.Capability.keypad_local);
        m_terminal.puts(InfoCmp.Capability.cursor_visible);
    }

    // ------------------------------------------------------------------------


    public void display() {
        // tput smcup; use alternate screen
        m_terminal.puts(InfoCmp.Capability.enter_ca_mode);
        m_terminal.puts(InfoCmp.Capability.keypad_xmit);
        m_terminal.puts(InfoCmp.Capability.cursor_invisible);

        enterFreePaintMode();
    }

    public void refresh() {
         m_terminal.writer().flush();
    }

    private void enterFreePaintMode() {
//        Attributes attributes = new Attributes(m_terminal.getAttributes());
//        attributes.setLocalFlags(EnumSet.of(Attributes.LocalFlag.ICANON, Attributes.LocalFlag.ECHO, Attributes.LocalFlag.IEXTEN), false);
//        attributes.setInputFlags(EnumSet.of(Attributes.InputFlag.IXON, Attributes.InputFlag.ICRNL, Attributes.InputFlag.INLCR), false);
//        attributes.setControlChar(Attributes.ControlChar.VMIN, 1);
//        attributes.setControlChar(Attributes.ControlChar.VTIME, 0);
//        attributes.setControlChar(Attributes.ControlChar.VINTR, 0);
//        m_terminal.setAttributes(attributes);

//        m_terminal.handle(Terminal.Signal.INT, this::handleSignal);
//        m_terminal.handle(Terminal.Signal.QUIT, this::handleSignal);
//        m_terminal.handle(Terminal.Signal.WINCH, this::handleSignal);
    }

    private void handleSignal(Terminal.Signal signal) {
        switch (signal) {
            case INT:
            case QUIT:
                m_keepRunning = false;
                break;
            case WINCH:
                // TODO: Window resize
                break;
        }
    }
}
