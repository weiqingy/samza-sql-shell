package org.apache.samza.tools.client.interfaces;



import java.util.List;


public interface SqlFunction {
    public String getName();

    public String getDescription();

    public List<String> getArgumentTypes();

    public String getReturnType();

    /**
     * Don't forget to implement toString()
     */
}
