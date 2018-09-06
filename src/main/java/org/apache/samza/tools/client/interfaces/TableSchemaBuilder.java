package org.apache.samza.tools.client.interfaces;

import java.util.ArrayList;
import java.util.List;

public class TableSchemaBuilder {
    private List<String> m_names = new ArrayList<>();
    private List<String> m_typeNames = new ArrayList<>();

    private TableSchemaBuilder() {
    }

    public static TableSchemaBuilder builder() {
        return new TableSchemaBuilder();
    }

    public TableSchemaBuilder appendColumn(String name, String typeName) {
        if(name == null || name.isEmpty() || typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException();

        m_names.add(name);
        m_typeNames.add(typeName);
        return this;
    }

    public TableSchemaBuilder appendColumns(List<String> names, List<String> typeNames) {
        if(names == null || names.size() == 0
                ||typeNames == null || typeNames.size() == 0
                || names.size() != typeNames.size())
            throw new IllegalArgumentException();

        m_names.addAll(names);
        m_typeNames.addAll(typeNames);

        return this;
    }

    public TableSchema toTableSchema() {
        return new TableSchema(m_names, m_typeNames);
    }
}
