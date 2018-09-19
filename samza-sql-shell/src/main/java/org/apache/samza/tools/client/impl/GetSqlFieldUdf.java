package org.apache.samza.tools.client.impl;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.apache.samza.config.Config;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.udfs.ScalarUdf;


  /**
   * UDF that extracts a field value from a nested SamzaSqlRelMessage.SamzaSqlRelRecord by recursively following a query path.
   * Note that the root object must be a SamzaSqlRelMessage.SamzaSqlRelRecord.
   *
   * Syntax for field specification:
   * <ul>
   *  <li> SamzaSqlRelMessage.SamzaSqlRelRecord/Map: <code> field.subfield </code> </li>
   *  <li> Array: <code> field[index] </code> </li>
 *  <li> Scalar types: <code> field </code> </li>
 * </ul>
 *
 * Example query: <code> pageViewEvent.requestHeader.properties.cookies[3].sessionKey </code>
 *
 * Above query extracts the sessionKey field from below nested record:
 *
 *   pageViewEvent (SamzaSqlRelMessage.SamzaSqlRelRecord)
 *     - requestHeader (SamzaSqlRelMessage.SamzaSqlRelRecord)
 *       - properties (Map)
 *         - cookies (Array)
 *           - sessionKey (Scalar)
 *
 */
public class GetSqlFieldUdf implements ScalarUdf<String> {
  @Override
  public void init(Config udfConfig) {
  }

  @Override
  public String execute(Object... args) {
    Object currentFieldOrValue = args[0];
    Validate.isTrue(currentFieldOrValue == null
        || currentFieldOrValue instanceof SamzaSqlRelMessage.SamzaSqlRelRecord);
    if (currentFieldOrValue != null && args.length > 1) {
      String [] fieldNameChain = ((String) args[1]).split("\\.");
      for (int i = 0; i < fieldNameChain.length && currentFieldOrValue != null; i++) {
        currentFieldOrValue = extractField(fieldNameChain[i], currentFieldOrValue);
      }
    }

    if (currentFieldOrValue != null) {
      return currentFieldOrValue.toString();
    }

    return null;
  }

  static Object extractField(String fieldName, Object current) {
    if (current instanceof SamzaSqlRelMessage.SamzaSqlRelRecord) {
      SamzaSqlRelMessage.SamzaSqlRelRecord record = (SamzaSqlRelMessage.SamzaSqlRelRecord) current;
      Validate.isTrue(record.getFieldNames().contains(fieldName),
          String.format("Invalid field %s in %s", fieldName, record));
      return record.getField(fieldName).orElse(null);
    } else if (current instanceof Map) {
      Map map = (Map) current;
      Validate.isTrue(map.containsKey(fieldName), String.format("Invalid field %s in %s", fieldName, map));
      return map.get(fieldName);
    } else if (current instanceof List && fieldName.endsWith("]")) {
      List list = (List) current;
      int index = Integer.parseInt(fieldName.substring(fieldName.indexOf("[") + 1, fieldName.length() - 1));
      return list.get(index);
    }

    throw new IllegalArgumentException(String.format(
        "Unsupported accessing operation for data type: %s with field: %s.", current.getClass(), fieldName));
  }
}