package org.apache.samza.tools.client.interfaces;

import java.util.ArrayList;
import java.util.List;

public class SqlSchemaBuilder {
    private List<String> m_names = new ArrayList<>();
    private List<String> m_typeName = new ArrayList<>();

    private SqlSchemaBuilder() {
    }

    public static SqlSchemaBuilder builder() {
        return new SqlSchemaBuilder();
    }

    public SqlSchemaBuilder addField(String name, String fieldType) {
        if(name == null || name.isEmpty() || fieldType == null)
            throw new IllegalArgumentException();

        m_names.add(name);
        m_typeName.add(fieldType);
        return this;
    }

    public SqlSchemaBuilder appendFields(List<String> names, List<String> typeNames) {
        if(names == null || names.size() == 0
                ||typeNames == null || typeNames.size() == 0
                || names.size() != typeNames.size())
            throw new IllegalArgumentException();

        m_names.addAll(names);
        m_typeName.addAll(typeNames);

        return this;
    }

    public SqlSchema toTableSchema() {
        return new SqlSchema(m_names, m_typeName);
    }
}
