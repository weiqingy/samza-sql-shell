package org.apache.samza.tools.client.impl;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.avro.AvroRelSchemaProvider;
import org.apache.samza.sql.avro.AvroTypeFactoryImpl;
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.RelSchemaProviderFactory;
import org.apache.samza.system.SystemStream;


public class FileSystemAvroRelSchemaProviderFactory implements RelSchemaProviderFactory {

  public static final String CFG_SCHEMA_DIR = "schemaDir";

  @Override
  public RelSchemaProvider create(SystemStream systemStream, Config config) {
    return new FileSystemAvroRelSchemaProvider(systemStream, config);
  }

  private class FileSystemAvroRelSchemaProvider implements AvroRelSchemaProvider {
    private final SystemStream systemStream;
    private final String schemaDir;

    public FileSystemAvroRelSchemaProvider(SystemStream systemStream, Config config) {
      this.systemStream = systemStream;
      this.schemaDir = config.get(CFG_SCHEMA_DIR);
    }

    @Override
    public RelDataType getRelationalSchema() {
      String schemaStr = this.getSchema(this.systemStream);
      Schema schema = Schema.parse(schemaStr);
      AvroTypeFactoryImpl avroTypeFactory = new AvroTypeFactoryImpl();
      return avroTypeFactory.createType(schema);
    }

    @Override
    public String getSchema(SystemStream systemStream) {
      String fileName = String.format("%s.avsc", systemStream.getStream());
      File file = new File(schemaDir, fileName);
      try {
        return Schema.parse(file).toString();
      } catch (IOException e) {
        throw new SamzaException(e);
      }
    }
  }
}
