package org.apache.samza.tools.client.cli;

import java.util.ArrayList;
import java.util.List;

/**
 * meta commands start with "." ????
 * extention commands statrt with "!"
 *
 */

enum CliCommandType {
    CLEAR           ("CLEAR", "\tClears the screen."),
    DESCRIBE        ("DESCRIBE", "Describes a table."),
    EXECUTE         ("EXECUTE", "\tExecute a sql file."),
    INSERT_INTO     ("INSERT INTO", "Executes a SQL INSERT INTO."),
    HELP            ("HELP", "\tDisplays this help message."),
    SELECT          ("SELECT", "\tExecutes a SQL SELECT query."),
    SET             ("SET", "\tSets a variable."),
    SHOW_TABLES     ("SHOW TABLES", "Shows all available tables."),
    QUIT            ("QUIT", "\tQuits the shell."),

    INVALID_COMMAND ("INVALID_COMMAND", "INVALID_COMMAND");

    private final String m_description;
    private final String m_cmdName;

    CliCommandType(String cmdName, String description) {
        m_cmdName = cmdName;
        m_description = description;
    }

    public String getCommandName() {
        return m_cmdName;
    }

    public String getDescription() {
        return m_description;
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