package org.apache.samza.tools.client.interfaces;


import java.util.List;

public class SqlSchema {
    private String[] m_names; // field names
    private String[] m_typeNames; // names of field type

    public SqlSchema(List<String> colNames, List<String> colTypeNames) {
        if(colNames == null || colNames.size() == 0
                ||colTypeNames == null || colTypeNames.size() == 0
                || colNames.size() != colTypeNames.size())
            throw new IllegalArgumentException();

        m_names = new String[colNames.size()];
        m_names = colNames.toArray(m_names);

        m_typeNames = new String[colTypeNames.size()];
        m_typeNames = colTypeNames.toArray(m_typeNames);
    }

    public int getFieldCount() {
        return m_names.length;
    }

    public String getFieldName(int colIdx) {
        return m_names[colIdx];
    }

    public String getFieldTypeName(int colIdx) {
        return m_typeNames[colIdx];
    }
}
