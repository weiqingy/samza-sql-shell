package org.apache.samza.tools.client.interfaces;


import java.util.List;

public class SamzaSqlSchema {
    private String[] m_names;

    // TODO: we may need concrete type information later
    private SamzaSqlFieldType[] m_typeNames;

    public SamzaSqlSchema(List<String> colNames, List<SamzaSqlFieldType> colTypeNames) {
        if(colNames == null || colNames.size() == 0
                ||colTypeNames == null || colTypeNames.size() == 0
                || colNames.size() != colTypeNames.size())
            throw new IllegalArgumentException();

        m_names = new String[colNames.size()];
        colNames.toArray(m_names);

        m_typeNames = new SamzaSqlFieldType[colTypeNames.size()];
        colTypeNames.toArray(m_typeNames);
    }

    public int getColumnCount() {
        return m_names.length;
    }

    public String getColumnName(int colIdx) {
        return m_names[colIdx];
    }

    public SamzaSqlFieldType getColumTypeName(int colIdx) {
        return m_typeNames[colIdx];
    }
}
