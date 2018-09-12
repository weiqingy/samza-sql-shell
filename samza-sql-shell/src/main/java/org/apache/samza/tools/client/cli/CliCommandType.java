package org.apache.samza.tools.client.cli;

import java.util.ArrayList;
import java.util.List;

/**
 * meta commands start with "." ????
 * extention commands statrt with "!"
 *
 */

enum CliCommandType {
    CLEAR           ("CLEAR", "\tClears the screen.", "CLEAR"),
    DESCRIBE        ("DESCRIBE", "Describes a table.", "Usage :DESCRIBE <table name>"),
    EXECUTE         ("EXECUTE", "\tExecute a sql file.", "EXECUTE <URI of a sql file>"),
    INSERT_INTO     ("INSERT INTO", "Executes a SQL INSERT INTO.", "INSERT INTO uses a standard streaming SQL syntax."),
    HELP            ("HELP", "\tDisplays this help message.", "Usage: HELP [command]"),
    SELECT          ("SELECT", "\tExecutes a SQL SELECT query.", "SELECT uses a standard streaming SQL syntax."),
    SET             ("SET", "\tSets a variable.", "Usage: SET VAR=VAL"),
    SHOW_TABLES     ("SHOW TABLES", "Shows all available tables.", "Usage: SHOW TABLES <table name>"),
    SHOW_FUNCTIONS  ("SHOW FUNCTIONS", "Shows all available UDFs.", "SHOW FUNCTION"),

    QUIT            ("QUIT", "\tQuits the shell.", "QUIT"),

    INVALID_COMMAND ("INVALID_COMMAND", "INVALID_COMMAND", "INVALID_COMMAND");

    private final String m_cmdName;
    private final String m_description;
    private final String m_usage;

    CliCommandType(String cmdName, String description, String usage) {
        m_cmdName = cmdName;
        m_description = description;
        m_usage = usage;
    }

    public String getCommandName() {
        return m_cmdName;
    }

    public String getDescription() {
        return m_description;
    }

    public String getUsage() {
        return m_usage;
    }


    public static List<String> getAllCommands() {
        List<String> cmds = new ArrayList<String>();
        for(CliCommandType t : CliCommandType.values()) {
            if(t != INVALID_COMMAND)
                cmds.add(t.getCommandName());
        }
        return cmds;
    }
}