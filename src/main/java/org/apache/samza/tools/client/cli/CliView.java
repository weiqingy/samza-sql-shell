package org.apache.samza.tools.client.cli;

import org.apache.samza.tools.client.interfaces.QueryResult;

public interface CliView {

    public void open(CliShell shell, QueryResult queryResult);
}
