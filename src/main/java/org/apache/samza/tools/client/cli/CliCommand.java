package org.apache.samza.tools.client.cli;


class CliCommand {
    private CliCommandType m_cmdType;
    private String m_parameters;

    public CliCommand(CliCommandType cmdType) {
        m_cmdType = cmdType;
    }

    public CliCommand(CliCommandType cmdType, String parameters) {
        this(cmdType);
        m_parameters = parameters;
    }

    public CliCommandType getCommandType() {
        return m_cmdType;
    }

    public String getParameters() {
        return m_parameters;
    }

    public void setParameters(String parameters) {
        m_parameters = parameters;
    }

    public String getFullCommand() {
        return m_cmdType.getCommandName() + CliConstants.SPACE + m_parameters;
    }
}