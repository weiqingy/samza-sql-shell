package org.apache.samza.tools.client.cli;

import java.io.*;
import java.util.*;

import org.apache.samza.SamzaException;
import org.apache.samza.tools.client.impl.SamzaExecutor;
import org.apache.samza.tools.client.impl.UdfDisplayInfo;
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

import java.net.URI;
import java.net.URISyntaxException;


class CliShell {
    private final Terminal m_terminal;
    private final PrintWriter m_writer;
    private final LineReader m_lineReader;
    private final String m_1stPrompt;
    private final SqlExecutor m_executor;
    private CliEnvironment m_env;
    private Map<Integer, String> m_executions = new HashMap<>();

    private boolean m_keepRunning = true;

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
                .eofOnEscapedNewLine(true)
                .eofOnUnclosedQuote(true);
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
        m_env = new CliEnvironment();
        m_executor = new SamzaExecutor();
        m_executor.start(m_env.generateExecutionContext());
    }

    public Terminal getTerminal() {
        return m_terminal;
    }

    public CliEnvironment getEnvironment() {
        return m_env;
    }

    public SqlExecutor getExecutor() {
        return m_executor;
    }

    /**
    *  Actually run the shell. Does not return until user choose to exit.
    */
    public void open() {
        // Remember we cannot enter alternate screen mode here as there is only one alternate
        // screen and we need it to show streaming results. Clear the screen instead.
        clearScreen();

        // We control terminal directly; Forbid any Java System.out and System.err stuff so
        // any underlying output will not mess up the console
        disableJavaSystemOutAndErr();
        m_writer.write(CliConstants.WELCOME_MESSAGE);

        try {
            // Check if jna.jar exists in class path
            try {
                ClassLoader.getSystemClassLoader().loadClass("com.sun.jna.NativeLibrary");
            } catch (ClassNotFoundException e) {
                // Something's wrong. It could be a dumb terminal if neither jna nor jansi lib is there
                m_writer.write("Warning: jna.jar does NOT exist. It may lead to a dumb shell or a performance hit.\n");
            }

            while (m_keepRunning) {
                String line;
                try {
                    line = m_lineReader.readLine(m_1stPrompt);
                } catch (UserInterruptException e) {
                    continue;
                } catch (EndOfFileException e) {
                    commandQuit();
                    break;
                }

                if (!CliUtil.isNullOrEmpty(line)) {
                    CliCommand command = parseLine(line);
                    if (command == null)
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
                        case EXIT:
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

                        case SHOW_FUNCTIONS:
                            commandShowFunctions(command);
                            break;

                        case HELP:
                            commandHelp(command);
                            break;

                        case INVALID_COMMAND:
                            printHelpMessage();
                            break;

                        default:
                            m_writer.write("UNDER DEVELOPEMENT. Command:" + command.getCommandType() + "\n");
                            m_writer.write("Parameters:" +
                                    (CliUtil.isNullOrEmpty(command.getParameters()) ? "NULL" : command.getParameters())
                                    + "\n\n");
                            m_writer.flush();
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace(m_writer);
        }

        m_writer.write("Cleaning up... ");
        m_writer.flush();
        m_executor.stop(m_env.generateExecutionContext());

        m_writer.write("Done.\nBye.\n\n");
        m_writer.flush();

        try {
            m_terminal.close();
        } catch (IOException e) {
            // Doesn't matter
        }
    }

    private void commandClear() {
        clearScreen();
    }

    private void commandDescribe(CliCommand command) {
        String parameters = command.getParameters();
        if(CliUtil.isNullOrEmpty(parameters)) {
            m_writer.println(command.getCommandType().getUsage());
            m_writer.println();
            m_writer.flush();
            return;
        }

        SqlSchema tableSchema = m_executor.getTableScema(m_env.generateExecutionContext(), parameters);

        if(tableSchema == null) {
            m_writer.println("Failed to get schema. Error: " + m_executor.getErrorMsg());
        }
        else {
            m_writer.println();
            List<String> lines = formatSchema4Display(tableSchema);
            for(String line : lines) {
                m_writer.println(line);
            }
        }
        m_writer.println();
        m_writer.flush();
    }

    private void commandSet(CliCommand command) {
        String param = command.getParameters();
        if(CliUtil.isNullOrEmpty(param)) {
            m_env.printAll(m_writer);
            return;
        }
        String[] params = param.split("=");
        if(params.length != 2) {
            m_writer.println(command.getCommandType().getUsage());
            m_writer.println();
            m_writer.flush();
            return;
        }

        int ret = m_env.setEnvironmentVariable(params[0], params[1]);
        if(ret == 0) {
            m_writer.print(params[0]);
            m_writer.print(" set to ");
            m_writer.println(params[1]);
        } else if(ret == -1) {
            m_writer.print("Unknow variable: ");
            m_writer.println(params[0]);
        } else if(ret == -2){
            m_writer.print("Invalid value: ");
            m_writer.println(params[1]);
        }

        m_writer.println();
        m_writer.flush();
    }

    private  void commandExecuteFile(CliCommand command) {
        String parameters = command.getParameters();
        if (parameters == null || parameters.isEmpty()) {
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
        if (!valid) {
            m_writer.println("Invalid URI.\n");
            m_writer.flush();
            return;
        }

        NonQueryResult nonQueryResult = m_executor.executeNonQuery(m_env.generateExecutionContext(), uri);
        if(nonQueryResult == null) {
            m_writer.println("Execution error: ");
            m_writer.println(m_executor.getErrorMsg());
            m_writer.println();
            m_writer.flush();
            return;
        }

        List<String> submittedStmts = nonQueryResult.getSubmittedStmts();
        List<String> nonsubmittedStmts = nonQueryResult.getNonSubmittedStmts();

        m_writer.println("Sql file submitted. Execution ID: " + nonQueryResult.getExecutionId());
        m_writer.println("Submitted statements: \n");
        if (submittedStmts == null || submittedStmts.size() == 0) {
            m_writer.println("\tNone.");
        } else {
            for (String statement : submittedStmts) {
                m_writer.print("\t");
                m_writer.println(statement);
            }
            m_writer.println();
        }

        if (nonsubmittedStmts != null && nonsubmittedStmts.size() != 0) {
            m_writer.println("Statements NOT submitted: \n");
            for(String statement : nonsubmittedStmts) {
                m_writer.print("\t");
                m_writer.println(statement);
            }
            m_writer.println();
        }

        m_writer.println("Note: All query statements in a sql file are NOT submitted.");
        m_writer.println();
        m_writer.flush();
    }

    private void commandInsertInto(CliCommand command) {
        // TODO: Remove the try catch blcok. Executor is not supposed to report error by exceptions
        try {
            NonQueryResult result = m_executor.executeNonQuery(m_env.generateExecutionContext(),
                    Collections.singletonList(command.getFullCommand()));

            if (result.succeeded()) {
                m_writer.print("Execution submitted successfully. Id: ");
                m_writer.println(String.valueOf(result.getExecutionId()));
//                m_writer.println();
            } else {
                m_writer.write("Execution failed to submit. Error: ");
                m_writer.write(m_executor.getErrorMsg());
                m_writer.println();
                m_writer.flush();
                return;
            }
        }
        catch(Exception e) {
            m_writer.println("Exception: " + e.getClass().getName());
            m_writer.println("Execution error: " + e.getMessage());
            m_writer.println();
            m_writer.flush();
            return;
        }

        // For Demo; Doesn't allow more operation unless the insert into is cancelled
        m_insertIntoRunning = true;
        m_writer.println("Press Ctrl + C to cancel the execution...");
        m_writer.flush();
        Terminal.SignalHandler handler_INT = m_terminal.handle(Terminal.Signal.INT, this::handleSignal);
        while(m_insertIntoRunning) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                break;
            }
        }
        // Restore original Ctrl C handler
        m_terminal.handle(Terminal.Signal.INT, handler_INT);
        m_writer.println();
        m_writer.flush();
    }

    private void commandQuit() {
        m_keepRunning = false;
    }

    private void commandSelect(CliCommand command) {
        String fullCmd = command.getFullCommand();
        ExecutionContext exeContext = m_env.generateExecutionContext();
        try {
            QueryResult queryResult = m_executor.executeQuery(exeContext, command.getFullCommand());
            m_executions.put(queryResult.getExecutionId(), fullCmd);

            CliView view = new QueryResultLogView();
            view.open(this, queryResult);
            m_executor.stopExecution(exeContext, queryResult.getExecutionId());
        } catch (SamzaException e) {
            m_writer.write("Failed to query. Error: ");
            m_writer.write(e.getMessage());
            m_writer.write("\n\n");
            e.printStackTrace();
        }
    }

    private void commandShowTables(CliCommand command) {
        List<String> tableNames = m_executor.listTables(m_env.generateExecutionContext());

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

    private void commandShowFunctions(CliCommand command) {
        List<SqlFunction> fns = m_executor.listFunctions(m_env.generateExecutionContext());

        if(fns != null) {
            for(SqlFunction fn : fns) {
                m_writer.write(fn.toString());
                m_writer.write('\n');
            }
            m_writer.write('\n');
        } else {
            m_writer.write("Failed to list functions. Error: ");
            m_writer.write(m_executor.getErrorMsg());
            m_writer.write("\n\n");
        }
        m_writer.flush();

    }

    private void commandHelp(CliCommand command) {
        String parameters = command.getParameters();
        if(parameters == null || parameters.isEmpty()) {
            printHelpMessage();
            return;
        }

        parameters = parameters.trim().toUpperCase();
        for (CliCommandType cmdType : CliCommandType.values()) {
            String cmdText =  cmdType.getCommandName();
            if(cmdText.equals(parameters)) {
                m_writer.println(cmdType.getUsage());
                m_writer.println();
                m_writer.flush();
                return;
            }
        }

        m_writer.print("Unknown command: ");
        m_writer.println(parameters);
        m_writer.println();
        m_writer.flush();
    }


    private CliCommand parseLine(String line) {
        line = CliUtil.trimCommand(line);
        if(CliUtil.isNullOrEmpty(line))
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
        m_writer.println();
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
        m_writer.println("HELP <COMMAND> to get help for a specific command.\n");
        m_writer.flush();
    }

    private void clearScreen() {
        m_terminal.puts(InfoCmp.Capability.clear_screen);
    }

    /*
        Field    | Type
        -------------------------
        Field1   | Type 1
        Field2   | VARCHAR(STRING)
        Field... | VARCHAR(STRING)
        -------------------------
    */
    private List<String> formatSchema4Display(SqlSchema tableSchema) {
        final String HEADER_FIELD = "Field";
        final String HEADER_TYPE = "Type";
        final char SEPERATOR = '|';
        final char LINE_SEP = '-';

        int terminalWidth = m_terminal.getWidth();
        // Two spaces * 2 plus one SEPERATOR
        if(terminalWidth < 2 + 2 + 1 + HEADER_FIELD.length() + HEADER_TYPE.length()) {
            return Collections.singletonList("Not enough room.");
        }

        // Find the best seperator position for least rows
        int seperatorPos = HEADER_FIELD.length() + 2;
        int minRowNeeded = Integer.MAX_VALUE;
        int longestLineCharNum = 0;
        int rowCount = tableSchema.getFieldCount();
        for(int j = seperatorPos; j < terminalWidth - HEADER_TYPE.length() - 2; ++j) {
            boolean fieldWrapped = false;
            int rowNeeded = 0;
            for (int i = 0; i < rowCount; ++i) {
                int fieldLen = tableSchema.getFieldName(i).length();
                int typeLen = tableSchema.getFieldTypeName(i).length();
                int fieldRowNeeded = CliUtil.ceilingDiv(fieldLen, j - 2);
                int typeRowNeeded = CliUtil.ceilingDiv(typeLen, terminalWidth - 1 - j - 2);

                rowNeeded += Math.max(fieldRowNeeded, typeRowNeeded);
                fieldWrapped |= fieldRowNeeded > 1;
                if(typeRowNeeded > 1) {
                    longestLineCharNum = terminalWidth;
                }
                else {
                    longestLineCharNum = Math.max(longestLineCharNum, j + typeLen + 2 + 1);
                }
            }
            if(rowNeeded < minRowNeeded) {
                minRowNeeded = rowNeeded;
                seperatorPos = j;
            }
            if(!fieldWrapped)
                break;
        }

        List<String> lines = new ArrayList<>(minRowNeeded + 4);

        // Header
        StringBuilder line = new StringBuilder(terminalWidth);
        line.append(CliConstants.SPACE);
        line.append(HEADER_FIELD);
        CliUtil.appendTo(line, seperatorPos - 1, CliConstants.SPACE);
        line.append(SEPERATOR);
        line.append(CliConstants.SPACE);
        line.append(HEADER_TYPE);
        lines.add(line.toString());
        line = new StringBuilder(terminalWidth);
        CliUtil.appendTo(line, longestLineCharNum - 1, LINE_SEP);
        lines.add(line.toString());

        // Body
        AttributedStyle oddLineStyle = AttributedStyle.DEFAULT.BOLD.foreground(AttributedStyle.BLUE);
        AttributedStyle evenLineStyle = AttributedStyle.DEFAULT.BOLD.foreground(AttributedStyle.CYAN);

        final int fieldColSize = seperatorPos - 2;
        final int typeColSize = terminalWidth - seperatorPos - 1 - 2;
        for (int i = 0; i < rowCount; ++i) {
            String field = tableSchema.getFieldName(i);
            String type = tableSchema.getFieldTypeName(i);
            int fieldLen = field.length();
            int typeLen = type.length();
            int fieldStartIdx = 0, typeStartIdx = 0;
            while(fieldStartIdx < fieldLen || typeStartIdx < typeLen) {
                line = new StringBuilder(terminalWidth);
                line.append(CliConstants.SPACE);
                int numToWrite = Math.min(fieldColSize, fieldLen - fieldStartIdx);
                if(numToWrite > 0) {
                    line.append(field, fieldStartIdx, fieldStartIdx + numToWrite);
                    fieldStartIdx += numToWrite;
                }
                CliUtil.appendTo(line, seperatorPos - 1, CliConstants.SPACE);
                line.append(SEPERATOR);
                line.append(CliConstants.SPACE);

                numToWrite = Math.min(typeColSize, typeLen - typeStartIdx);
                if(numToWrite > 0) {
                    line.append(type, typeStartIdx, typeStartIdx + numToWrite);
                    typeStartIdx += numToWrite;
                }

                if(i % 2 == 0) {
                    AttributedStringBuilder attrBuilder = new AttributedStringBuilder().style(evenLineStyle);
                    attrBuilder.append(line.toString());
                    lines.add(attrBuilder.toAnsi());
                } else {
                    AttributedStringBuilder attrBuilder = new AttributedStringBuilder().style(oddLineStyle);
                    attrBuilder.append(line.toString());
                    lines.add(attrBuilder.toAnsi());
                }
            }
        }

        // Footer
        line = new StringBuilder(terminalWidth);
        CliUtil.appendTo(line, longestLineCharNum - 1, LINE_SEP);
        lines.add(line.toString());
        return lines;
    }

    private void disableJavaSystemOutAndErr() {
        PrintStream ps = new PrintStream(new NullOutputStream());
        System.setOut(ps);
        System.setErr(ps);
    }

    private class NullOutputStream extends OutputStream {
        public void close() {}
        public void flush() {}
        public void write(byte[] b) {}
        public void write(byte[] b, int off, int len) {}
        public void write(int b) {}
    }

    // TODO: REMOVE
    // Hack for demo
    private volatile boolean m_insertIntoRunning = false;
    private void handleSignal(Terminal.Signal signal) {
        switch (signal) {
            case INT:
                m_insertIntoRunning = false;
                break;
        }
    }
}
