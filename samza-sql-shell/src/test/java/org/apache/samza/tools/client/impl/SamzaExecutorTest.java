package org.apache.samza.tools.client.impl;


import java.util.List;
import org.apache.samza.tools.client.interfaces.QueryResult;
import org.apache.samza.tools.client.interfaces.SqlSchema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class SamzaExecutorTest {
    private SamzaExecutor m_executor = new SamzaExecutor();

    /**
     * Need a local Kafka cluster
     */
    @Ignore
    @Test
    public void testQueryResult() {
        String sql = "select * from kafka.ProfileChangeStream";
        QueryResult queryResult = m_executor.executeQuery(null, sql);
        SqlSchema ts = queryResult.getTableSchema();

        Assert.assertEquals("__key__", ts.getColumnName(0));
        Assert.assertEquals("Name", ts.getColumnName(1));
        Assert.assertEquals("NewCompany", ts.getColumnName(2));
        Assert.assertEquals("OldCompany", ts.getColumnName(3));
        Assert.assertEquals("ProfileChangeTimestamp", ts.getColumnName(4));

        /*Assert.assertEquals("VARCHAR", ts.getColumTypeName(0));
        Assert.assertEquals("VARCHAR", ts.getColumTypeName(1));
        Assert.assertEquals("VARCHAR", ts.getColumTypeName(2));
        Assert.assertEquals("VARCHAR", ts.getColumTypeName(3));
        Assert.assertEquals("BIGINT", ts.getColumTypeName(4));*/

        try {
          Thread.sleep(5000); // wait for seconds
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        List<String[]> data = m_executor.retrieveQueryResult(null, 1, 2);
        Assert.assertEquals(2, data.size());

        m_executor.stop(null);
    }

    // -- TODO: end to end testing. We can use TestAvroSystemFactory
    /* @Test
    public void testRetrieveQueryResult() {

    }

    @Test
    public void testConsumeQueryResult() {

    }

    @Test
    public void testExecuteQuery() {

    }

    @Test
    public void testGetRowCount() {

    }

    @Test
    public void testExecuteNonQuery() {

    }

    @Test
    public void testRemoveExecution() {

    }

    @Test
    public void queryExecutionStatus() {

    }*/
}
