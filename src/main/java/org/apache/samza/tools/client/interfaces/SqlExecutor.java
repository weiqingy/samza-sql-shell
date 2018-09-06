package org.apache.samza.tools.client.interfaces;


import java.util.List;

/**
 *  Conventions:
 *
 *  Implementations shall report UNRECOVERABLE EXCEPTIONS by throwing
 *  ExecutionExceptions, though SqlExecutor doesn't enforce this by as we don't believe in
 *  Java checked exceptions. Report errors by returning values as indicated by each
 *  function and preparing for the subsequent getErrorMsg call.
 *
 *  Each execution (both query and non-query shall return an non-negative execution ID(execId).
 *  Negative execution IDs are reserved for error handling.
 *
 *  User shall be able to query the status of an execution even after it's finished, so the
 *  executor shall keep record of all the execution unless being asked to remove them (
 *  when removeExecution is called.)
 *
 *
 */
public interface SqlExecutor {
    /**
     *  SqlExecutor shall be ready to accept all other calls after start() is called.
     *  However, it shall NOT store the ExecutionContext for future use, as each
     *  call will be given an ExecutionContext which may differ from this one.
     */
    public void start(ExecutionContext context);

    /**
     * Indicates no further calls will be made thus it's safe for the executor to clean up.
     */
    public void stop(ExecutionContext context);

    /**
     * @return null if an error occurs. Prepare for subsequent getErrorMsg call.
     *         an empty list indicates no tables found.
     */
    public List<String> listTables(ExecutionContext context);

    /**
     * @return null if an error occurs. Prepare for subsequent getErrorMsg call.
     */
    public TableSchema getTableScema(ExecutionContext context, String tableName);

    /**
     */
    public QueryResult executeQuery(ExecutionContext context, String statement);

    /**
     * @return how many rows for reading.
     */
    public int getRowCount();

    /**
     * Row starts at 0. Executor shall keep the data retrieved.
     * For now we get strings for display but we might want strong typed values.
     */
    public List<String[]> retrieveQueryResult(ExecutionContext context, int startRow, int endRow);


    /**
     * Consumes rows from query result. Executor shall drop them, as "consume" indicates.
     * @param count 0 or negative number means all data.
     * @return
     */
    // For loging view mode. Still not sure what the interface should be like.
    // Don't support thie method for now.
    // public List<String[]> consumeQueryResult(ExecutionContext context, int count);

    /**
     */
    public NonQueryResult executeNonQuery(ExecutionContext context, List<String> statement);

    /**
     */
    public boolean stopExecution(ExecutionContext context, int exeId);


    /**
     *  Removing an ongoing execution shall result in an error. Stop it first.
     */
    public boolean removeExecution(ExecutionContext context, int exeId);

    /**
     *
     */
    public ExecutionStatus queryExecutionStatus(int execId);

    /**
     *
     */
    public String getErrorMsg();
}
