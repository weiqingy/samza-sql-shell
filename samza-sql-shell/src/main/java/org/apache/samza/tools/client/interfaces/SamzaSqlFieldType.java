package org.apache.samza.tools.client.interfaces;

public class SamzaSqlFieldType {

  public boolean isPrimitiveField() {
    return typeName != TypeName.ARRAY && typeName != TypeName.MAP && typeName != TypeName.ROW;
  }

  public enum TypeName {
    BYTE, // One-byte signed integer.
    INT16, // two-byte signed integer.
    INT32, // four-byte signed integer.
    INT64, // eight-byte signed integer.
    DECIMAL, // Decimal integer
    FLOAT,
    DOUBLE,
    STRING, // String.
    DATETIME, // Date and time.
    BOOLEAN, // Boolean.
    BYTES, // Byte array.
    ARRAY,
    MAP,
    ROW; // The field is itself a nested row.
  }

  public static SamzaSqlFieldType createPrimitiveFieldType(TypeName typeName) {
    return new SamzaSqlFieldType(typeName, null, null, null);
  }

  public static SamzaSqlFieldType createArrayFieldType(SamzaSqlFieldType elementType) {
    return new SamzaSqlFieldType(TypeName.ARRAY, elementType, null, null);
  }

  public static SamzaSqlFieldType createMapFieldType(SamzaSqlFieldType valueType) {
    return new SamzaSqlFieldType(TypeName.MAP, null, valueType, null);
  }

  public static SamzaSqlFieldType createRowFieldType(SamzaSqlSchema rowSchema) {
    return new SamzaSqlFieldType(TypeName.ROW, null, null, rowSchema);
  }

  private SamzaSqlFieldType(TypeName typeName, SamzaSqlFieldType elementType, SamzaSqlFieldType valueType, SamzaSqlSchema rowSchema) {
    this.typeName = typeName;
    this.elementType = elementType;
    this.valueType = valueType;
    this.rowSchema = rowSchema;
  }

  private TypeName typeName;

  private SamzaSqlFieldType elementType;

  private SamzaSqlFieldType valueType;

  private SamzaSqlSchema rowSchema;

  public TypeName getTypeName() {
    return typeName;
  }

  public SamzaSqlFieldType getElementType() {
    return elementType;
  }

  public SamzaSqlFieldType getValueType() {
    return valueType;
  }

  public SamzaSqlSchema getRowSchema() {
    return rowSchema;
  }
}
