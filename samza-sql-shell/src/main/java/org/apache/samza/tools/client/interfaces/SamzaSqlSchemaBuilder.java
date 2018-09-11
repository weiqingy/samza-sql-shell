package org.apache.samza.tools.client.interfaces;

import java.util.ArrayList;
import java.util.List;

public class SamzaSqlSchemaBuilder {
    private List<String> m_names = new ArrayList<>();
    private List<SamzaSqlFieldType> m_fieldTypes = new ArrayList<>();

    private SamzaSqlSchemaBuilder() {
    }

    public static SamzaSqlSchemaBuilder builder() {
        return new SamzaSqlSchemaBuilder();
    }

    public SamzaSqlSchemaBuilder addField(String name, SamzaSqlFieldType fieldType) {
        if(name == null || name.isEmpty() || fieldType == null)
            throw new IllegalArgumentException();

        m_names.add(name);
        m_fieldTypes.add(fieldType);
        return this;
    }

    public SamzaSqlSchemaBuilder appendColumns(List<String> names, List<SamzaSqlFieldType> typeNames) {
        if(names == null || names.size() == 0
                ||typeNames == null || typeNames.size() == 0
                || names.size() != typeNames.size())
            throw new IllegalArgumentException();

        m_names.addAll(names);
        m_fieldTypes.addAll(typeNames);

        return this;
    }

    public SamzaSqlSchema toTableSchema() {
        return new SamzaSqlSchema(m_names, m_fieldTypes);
    }
}
