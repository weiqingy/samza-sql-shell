package org.apache.samza.tools.client.cli;

import org.apache.samza.tools.client.interfaces.ExecutionContext;
import org.apache.samza.tools.client.interfaces.QueryResult;
import org.apache.samza.tools.client.interfaces.SqlExecutor;
import org.apache.samza.tools.client.util.CliUtil;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Attributes;
import org.jline.terminal.Cursor;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.utils.*;

import java.time.LocalTime;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;


/**
 *  Refer to OReilly's Posix Programming Guide Chapter 8, Terminal I/O and termios(3) for terminal control
 */


public class QueryResultExpendedLogView implements CliView {
    private static final int DEFAULT_REFRESH_INTERVAL = 1000; // all intervals are in ms

    private int m_refreshInterval = DEFAULT_REFRESH_INTERVAL;
    private LocalTime m_lastRefreshTime;
    private int m_width;
    private int m_height;

    private CliShell m_shell;
    private QueryResult m_queryResult;
    private Terminal  m_terminal;
    private SqlExecutor m_executor;
    private ExecutionContext m_exeContext;
    private volatile boolean m_keepRunning = true;

    private int m_counter;

    public QueryResultExpendedLogView() {

    }

    // -- implementation of CliView -------------------------------------------

    public void open(CliShell shell, QueryResult queryResult) {
        m_shell = shell;
        m_queryResult = queryResult;
        m_terminal = shell.getTerminal();
        m_executor = shell.getExecutor();
        m_exeContext = shell.getExecutionContext();

        TerminalStatus prevStatus = setupTerminal();
        updateTerminalSize();

        while(m_keepRunning) {
            try {
                boolean needsRefresh = true;
                if(needsRefresh) {
                    display();
                }
                Thread.sleep(m_refreshInterval);
            } catch (InterruptedException e) {
                continue;
            }
        }

        restoreTerminal(prevStatus);
    }

    // ------------------------------------------------------------------------


    private void display() {
        updateTerminalSize();

        // Clear status bar
        m_terminal.puts(InfoCmp.Capability.save_cursor);
        m_terminal.puts(InfoCmp.Capability.cursor_address, m_height - 1, 0);
        m_terminal.puts(InfoCmp.Capability.delete_line, m_height - 1, 0);
        m_terminal.puts(InfoCmp.Capability.restore_cursor);

        for(int i = 0; i < 70; ++i) {
            m_terminal.writer().println("Test Message " + m_counter++);
//            m_terminal.flush();
//            try {
//                Thread.sleep(500);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }

        m_terminal.puts(InfoCmp.Capability.save_cursor);
        m_terminal.puts(InfoCmp.Capability.cursor_address, m_height - 1, 0);

        // print status bar
        int rowsBehind = 1000;
        boolean paused = true;
        AttributedStyle statusBarStyle = AttributedStyle.DEFAULT.background(AttributedStyle.WHITE)
                .foreground(AttributedStyle.BLACK);
        AttributedStringBuilder attrBuilder = new AttributedStringBuilder()
                .style(statusBarStyle.bold().italic())
                .append("Q")
                .style(statusBarStyle)
                .append(": Quit     ")
                .style(statusBarStyle.bold().italic())
                .append("SPACE")
                .style(statusBarStyle)
                .append(": Pause/Resume     ")
                .append(String.valueOf(rowsBehind) + " rows in buffer     ");
        if(paused) {
            attrBuilder.style(statusBarStyle.bold().foreground(AttributedStyle.RED).blink())
                    .append("PAUSED");
        }


        String statusBarText = attrBuilder.toAnsi();
        m_terminal.writer().print(statusBarText);

        m_terminal.puts(InfoCmp.Capability.restore_cursor);
        m_terminal.flush();
    }

    private TerminalStatus setupTerminal() {
        TerminalStatus prevStatus = new TerminalStatus();

        // Signal handlers
        prevStatus.handler_INT = m_terminal.handle(Terminal.Signal.INT, this::handleSignal);
        prevStatus.handler_QUIT = m_terminal.handle(Terminal.Signal.QUIT, this::handleSignal);
        prevStatus.handler_TSTP = m_terminal.handle(Terminal.Signal.TSTP, this::handleSignal);
        prevStatus.handler_CONT = m_terminal.handle(Terminal.Signal.CONT, this::handleSignal);;
        prevStatus.handler_WINCH = m_terminal.handle(Terminal.Signal.WINCH, this::handleSignal);

        // Attributes
        prevStatus.attributes = m_terminal.getAttributes();
        Attributes newAttributes = new Attributes(prevStatus.attributes);
        // (003, ETX, Ctrl-C, or also 0177, DEL, rubout) Interrupt char‐
        // acter (INTR).  Send a SIGINT signal.  Recognized when ISIG is
        // set, and then not passed as input.
 //       newAttributes.setControlChar(Attributes.ControlChar.VINTR, 1);
        // (034, FS, Ctrl-\) Quit character (QUIT).  Send SIGQUIT signal.
        // Recognized when ISIG is set, and then not passed as input.
//        newAttributes.setControlChar(Attributes.ControlChar.VQUIT, 0);
        newAttributes.setControlChar(Attributes.ControlChar.VMIN, 1);
        newAttributes.setControlChar(Attributes.ControlChar.VTIME, 0);
        // Enables signals and SIGTTOU signal to the process group of a background
        // process which tries to write to our terminal
        newAttributes.setLocalFlags(
                EnumSet.of(Attributes.LocalFlag.ISIG, Attributes.LocalFlag.TOSTOP), true);
        // No canonical mode, no echo, and no implementation-defined input processing
        newAttributes.setLocalFlags(EnumSet.of(
                Attributes.LocalFlag.ICANON, Attributes.LocalFlag.ECHO,
                Attributes.LocalFlag.IEXTEN), false);
        // Input flags
        newAttributes.setInputFlags(EnumSet.of(
                Attributes.InputFlag.ICRNL, Attributes.InputFlag.INLCR, Attributes.InputFlag.IXON), false);
        m_terminal.setAttributes(newAttributes);

        // Capabilities
        // tput smcup; use alternate screen
        m_terminal.puts(InfoCmp.Capability.enter_ca_mode);
        m_terminal.puts(InfoCmp.Capability.cursor_invisible);
        m_terminal.puts(InfoCmp.Capability.cursor_home);

        m_terminal.flush();

        return prevStatus;
    }

    private void restoreTerminal(TerminalStatus status) {
        // Signal handlers
        m_terminal.handle(Terminal.Signal.INT, status.handler_INT);
        m_terminal.handle(Terminal.Signal.QUIT, status.handler_QUIT);
        m_terminal.handle(Terminal.Signal.TSTP, status.handler_TSTP);
        m_terminal.handle(Terminal.Signal.CONT, status.handler_CONT);
        m_terminal.handle(Terminal.Signal.WINCH, status.handler_WINCH);

        // Attributes
        m_terminal.setAttributes(status.attributes);

        // Capability
        m_terminal.puts(InfoCmp.Capability.exit_ca_mode);
        m_terminal.puts(InfoCmp.Capability.cursor_visible);
    }

    private void handleSignal(Terminal.Signal signal) {
        switch (signal) {
            case INT:
            case QUIT:
                m_keepRunning = false;
                break;
            case TSTP:
                // Pause
                break;
            case CONT:
                // continue
                break;
            case WINCH:
                updateTerminalSize();
                break;
        }
    }

    private static class TerminalStatus {
        Terminal.SignalHandler handler_INT;
        Terminal.SignalHandler handler_QUIT;
        Terminal.SignalHandler handler_TSTP;
        Terminal.SignalHandler handler_CONT;
        Terminal.SignalHandler handler_WINCH;

        Attributes attributes;
    }

    private void updateTerminalSize() {
        m_terminal.flush();
        m_width = m_terminal.getWidth();
        m_height = m_terminal.getHeight();
    }
}
