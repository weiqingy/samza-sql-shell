package org.apache.samza.tools.client.cli;

import org.apache.samza.SamzaException;
import org.apache.samza.tools.client.impl.SamzaExecutor;
import org.apache.samza.tools.client.interfaces.*;
import org.apache.samza.tools.client.util.CliException;
import org.apache.samza.tools.client.util.CliUtil;

import org.jline.reader.*;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.reader.impl.DefaultParser;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


class CliShell {
    private final Terminal m_terminal;
    private final PrintWriter m_writer;
    private final LineReader m_lineReader;
    private final String m_1stPrompt;
    private final SqlExecutor m_executor;
    private ExecutionContext m_exeContext;
    private Map<Integer, String> m_executions = new HashMap<>();

    private boolean m_keepRunning = true;

    // TODO: HACK CODE; REMOVE LATER
    private volatile boolean m_executorRunning_TMP = false;


    public CliShell() {
        // Terminal
        try {
            m_terminal = TerminalBuilder.builder()
                    .name(CliConstants.WINDOW_TITLE)
                    .build();
        }
        catch(IOException e) {
            throw new CliException("Error when creating terminal", e);
        }

        // Terminal writer
        m_writer = m_terminal.writer();

        // LineReader
        final DefaultParser parser = new DefaultParser()
                .eofOnEscapedNewLine(true);
        //        .eofOnUnclosedQuote(true);
        m_lineReader = LineReaderBuilder.builder()
                .appName(CliConstants.APP_NAME)
                .terminal(m_terminal)
                .parser(parser)
                .highlighter(new CliHighlighter())
                .completer(new StringsCompleter(CliCommandType.getAllCommands()))
                .build();

        // Command Prompt
        m_1stPrompt = new AttributedStringBuilder()
                .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW))
                .append(CliConstants.PROMPT_1ST + CliConstants.PROMPT_1ST_END)
                .toAnsi();

        // Execution context and executor
        m_exeContext = new ExecutionContext();
        m_executor = new SamzaExecutor();
        m_executor.start(m_exeContext);
    }

    public Terminal getTerminal() {
        return m_terminal;
    }

    public ExecutionContext getExecutionContext() {
        return m_exeContext;
    }

    public SqlExecutor getExecutor() {
        return m_executor;
    }

    /**
    *  Actually run the shell. Does not return until commanded.
    */
    public void open() {
        // Remember we cannot enter alternate screen mode here as there is only one alternate
        // screen and we need it to show streaming results. Clear the screen instead.

        // m_terminal.puts(InfoCmp.Capability.enter_ca_mode); // tput smcup
        // put the following line at the end of the method if the line above is enabled
        // m_terminal.puts(InfoCmp.Capability.exit_ca_mode); // tput rmcup
        clearScreen();
        m_writer.write(CliConstants.WELCOME_MESSAGE);

        // Check if jna.jar exists in class path
        try {
            ClassLoader.getSystemClassLoader().loadClass("com.sun.jna.NativeLibrary");
        } catch(ClassNotFoundException e) {
            // Something's wrong. It could be a dumb terminal if neither jna nor jansi lib is there
            // Going to a log? Not now.
            m_writer.write("Warning: jna.jar does NOT exist. It may lead to a dumb shell or a performance hit.\n");
        }

        while(m_keepRunning) {
            String line;
            try {
                line = m_lineReader.readLine(m_1stPrompt);
            } catch(UserInterruptException e) {
                m_writer.write("User interrupted. ");
                m_writer.flush();
                break;
            } catch(EndOfFileException e) {
                commandQuit();
                break;
            }

            if(!CliUtil.isStringNullOrEmpty(line)) {
                CliCommand command = parseLine(line);
                if(command == null)
                    continue;

                switch (command.getCommandType()) {
                    case CLEAR:
                        commandClear();
                        break;

                    case DESCRIBE:
                        commandDescribe(command);
                        break;

                    case EXECUTE:
                        commandExecuteFile(command);
                        break;

                    case INSERT_INTO:
                        commandInsertInto(command);
                        break;

                    case QUIT:
                        commandQuit();
                        break;

                    case SELECT:
                        commandSelect(command);
                        break;

                    case SET:
                        commandSet(command);
                        break;

                    case SHOW_TABLES:
                        commandShowTables(command);
                        break;

                    case HELP:
                    case INVALID_COMMAND:
                        printHelpMessage();
                        break;

                    default:
                        m_writer.write("TODO: Execute command:" + command.getCommandType() + "\n");
                        m_writer.write("Parameters:" +
                                (CliUtil.isStringNullOrEmpty(command.getParameters()) ? "NULL" : command.getParameters())
                                + "\n\n");
                        m_writer.flush();
                }
            }
        }

        m_writer.write("Cleaning up... ");
        m_writer.flush();
        m_executor.stop(m_exeContext);

        try {
            m_terminal.close();
        }  catch (IOException e) {
            // Doesn't matter
        }

        m_writer.write("Done.\nBye.\n\n");
        m_writer.flush();
    }

    private void commandClear() {
        clearScreen();
    }

    private void commandDescribe(CliCommand command) {
        // TODO: Remove the try catch blcok. Executor is not supposed to report error by exceptions
        try {
            TableSchema tableSchema = m_executor.getTableScema(m_exeContext, command.getParameters());
            printSchema(tableSchema);
        } catch(Exception e) {
            m_writer.println("Execution error: " + e.getMessage());
            m_writer.println("Exception: " + e.getClass().getName());
        }
        m_writer.flush();
    }

    private void commandSet(CliCommand command) {
        String param = command.getParameters();
        if(param == null) {
            m_writer.println("OUTPUT=" + m_exeContext.getMessageFormat());
            m_writer.println();
            m_writer.flush();
            return;
        }
        param = param.toUpperCase();
        String[] params = param.split("=");
        if(params.length != 2) {
            m_writer.println("Usage: SET VAR=VAL\n");
            m_writer.flush();
            return;
        }

        // TODO: When more set parameters come, move the code to a seperate class
        switch (params[0]) {
            case "OUTPUT":
                if(params[1].equals("PRETTY")) {
                    m_exeContext.setMessageFormat(ExecutionContext.MessageFormat.PRETTY);
                }
                else if (params[1].equals("COMPACT")) {
                    m_exeContext.setMessageFormat(ExecutionContext.MessageFormat.COMPACT);
                }
                else {
                    m_writer.println("possible value for OUTPUT: PRETTY, COMPACT\n");
                    m_writer.flush();
                }
                break;
            default:
                m_writer.println("Current supported variables:\n");
                m_writer.println("OUTPUT\t\tPRETTY|COMPACT\n");
                m_writer.flush();
        }
    }

    private  void commandExecuteFile(CliCommand command) {
        String parameters = command.getParameters();
        if(parameters == null || parameters.isEmpty()) {
            m_writer.println("Usage: execute <fileuri>\n");
            m_writer.flush();
            return;
        }
        URI uri = null;
        boolean valid = false;
        try {
            uri = new URI(parameters);
            File file = new File(uri.getPath());
            valid = file.exists();
        } catch (URISyntaxException e) {
        }
        if(!valid) {
            m_writer.println("Invalid URI.\n");
            m_writer.flush();
            return;
        }

        try {
            NonQueryResult nonQueryResult = m_executor.executeNonQuery(m_exeContext, uri);
        }
        catch (Exception e) {
            m_writer.println("Execution error: " + e.getMessage());
            m_writer.println("Exception: " + e.getClass().getName());
            m_writer.println();
        }
    }

    private void commandInsertInto(CliCommand command) {
        // TODO: Remove the try catch blcok. Executor is not supposed to report error by exceptions
        try {
            NonQueryResult result = m_executor.executeNonQuery(m_exeContext,
                    Collections.singletonList(command.getFullCommand()));

            if (result.succeeded()) {
                m_writer.write("Execution submitted successfully. Id: ");
                m_writer.write(String.valueOf(result.getExecutionId()));
                m_writer.write("  \n");
            } else {
                m_writer.write("Execution failed to submit. Error: ");
                m_writer.write(m_executor.getErrorMsg());
                m_writer.println();
            }
        }
        catch(Exception e) {
            m_writer.println("Execution error: " + e.getMessage());
            m_writer.println("Exception: " + e.getClass().getName());
            m_writer.println();
        }
        m_writer.println();
        m_writer.flush();

    }

    private void commandQuit() {
        m_keepRunning = false;
    }

    private void commandSelect(CliCommand command) {
        String fullCmd = command.getFullCommand();

/*
        // This code snippet is for temporary select implementation purpose
        // For now the executor prints out to screen directly which is unacceptable
        Terminal.SignalHandler handler_INT = m_terminal.handle(Terminal.Signal.INT, this::handleSignal);
        Terminal.SignalHandler handler_QUIT = m_terminal.handle(Terminal.Signal.QUIT, this::handleSignal);;

        // TODO: Remove the try catch blcok. Executor is not supposed to report error by exceptions
        try {
            m_executor.executeQuery(m_exeContext, fullCmd);
            m_executorRunning_TMP = true;
            while(m_executorRunning_TMP) {
                Thread.sleep(50);
            }
        } catch(Exception e) {
            m_terminal.writer().println("Execution error: " + e.getMessage());
            m_terminal.writer().println("Exception: " + e.getClass().getName());
        }

        m_terminal.handle(Terminal.Signal.INT, handler_INT);
        m_terminal.handle(Terminal.Signal.QUIT, handler_QUIT);
*/
        try {
            QueryResult queryResult = m_executor.executeQuery(m_exeContext, command.getFullCommand());
            m_executions.put(queryResult.getExecutionId(), fullCmd);

            CliView view = new QueryResultExpendedLogView();
            view.open(this, queryResult);
            m_executor.stopExecution(m_exeContext, queryResult.getExecutionId());
        } catch (SamzaException e) {
            m_writer.write("Failed to query. Error: ");
            m_writer.write(e.getMessage());
            m_writer.write("\n\n");
        }
    }

    private void commandShowTables(CliCommand command) {
        List<String> tableNames = m_executor.listTables(m_exeContext);

        if(tableNames != null) {
            for(String tableName : tableNames) {
                m_writer.write(tableName);
                m_writer.write('\n');
            }
            m_writer.write('\n');
        } else {
            m_writer.write("Failed to list tables. Error: ");
            m_writer.write(m_executor.getErrorMsg());
            m_writer.write("\n\n");
        }
        m_writer.flush();
    }

    private CliCommand parseLine(String line) {
        line = CliUtil.trimCommand(line);
        if(CliUtil.isStringNullOrEmpty(line))
            return null;

        String upperCaseLine = line.toUpperCase();
        for(CliCommandType cmdType : CliCommandType.values()) {
            String cmdText =  cmdType.getCommandName();
            if(upperCaseLine.startsWith(cmdText)) {
                if(upperCaseLine.length() == cmdText.length())
                    return new CliCommand(cmdType);
                else if(upperCaseLine.charAt(cmdText.length()) <= CliConstants.SPACE) {
                    String parameter = line.substring(cmdText.length()).trim();
                    if(!parameter.isEmpty())
                        return new CliCommand(cmdType, parameter);
                }
            }
        }
        return new CliCommand(CliCommandType.INVALID_COMMAND);
    }

    private void printHelpMessage() {
        AttributedStringBuilder builder = new AttributedStringBuilder();
        builder.append("The following commands are supported by ")
                .append(CliConstants.APP_NAME)
                .append(" at the moment.\n\n");

        for (CliCommandType cmdType : CliCommandType.values()) {
            if (cmdType == CliCommandType.INVALID_COMMAND)
                continue;

            String cmdText = cmdType.getCommandName();
            String cmdDescription = cmdType.getDescription();

            builder.style(AttributedStyle.DEFAULT.bold())
                    .append(cmdText)
                    .append("\t\t")
                    .style(AttributedStyle.DEFAULT)
                    .append(cmdDescription)
                    .append("\n");
        }

        m_writer.println(builder.toAnsi());
        m_writer.flush();
    }

    private void clearScreen() {
        m_terminal.puts(InfoCmp.Capability.clear_screen);
    }

    private void printSchema(TableSchema tableSchema) {
        if(tableSchema == null) {
            m_writer.write("Failed to get table schema. Error: ");
            m_writer.write(m_executor.getErrorMsg());
            m_writer.write("\n\n");
            return;
        }

/*
        Field   | Type
        -------------------------
        ROWTIME | BIGINT
        ROWKEY  | VARCHAR(STRING)
        IP      | VARCHAR(STRING)
        KBYTES  | BIGINT
        -------------------------
*/
        m_writer.println();

        int seperatorPos = 10;
        int terminalWidth = m_terminal.getWidth();
        int maxLineLength = terminalWidth;
        int count = tableSchema.getColumnCount();
        for (int i = 0; i < count; ++i) {
            String fieldName = tableSchema.getColumnName(i);
            seperatorPos = Math.max(fieldName.length() + 1, seperatorPos);
        }
        seperatorPos = Math.min(seperatorPos, terminalWidth / 2);
        for (int i = 0; i < count; ++i) {
            String typeName = tableSchema.getColumTypeName(i);
            maxLineLength = Math.max(typeName.length() + seperatorPos, seperatorPos);
        }
        maxLineLength += 6;
        maxLineLength = Math.min(maxLineLength, terminalWidth);
        for (int i = -1; i < count; ++i) {
            m_writer.write(' ');
            String fieldName = i == -1 ? "Field" : tableSchema.getColumnName(i);
            for(int j = 0; j < seperatorPos - 1; ++j) {
                m_writer.write(j < fieldName.length() ? fieldName.charAt(j) : ' ' );
            }
            m_writer.write(" | ");
            m_writer.println(i == -1 ? "Type" : tableSchema.getColumTypeName(i));

            if(i == -1 || i == count - 1) {
                for(int j = 0; j < maxLineLength; ++j) {
                    m_writer.write('-');
                }
                m_writer.println();
            }

        }
        m_writer.flush();
    }


    // TODO: REMOVE LATER; Hack for temp select execution
    private void handleSignal(Terminal.Signal signal) {
        switch (signal) {
            case INT:
            case QUIT:
                m_executor.stop(m_exeContext);
                m_terminal.writer().println("User cancelled query. \n");
                m_terminal.flush();
                m_executorRunning_TMP = false;
                break;
        }
    }
}
