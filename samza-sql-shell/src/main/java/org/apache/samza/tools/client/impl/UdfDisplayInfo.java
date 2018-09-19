package org.apache.samza.tools.client.impl;

import com.google.common.base.Joiner;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.samza.tools.client.impl.SamzaSqlFieldType;
import org.apache.samza.tools.client.interfaces.SqlFunction;


public class UdfDisplayInfo implements SqlFunction {

  private String name;

  private String description;

  private List<SamzaSqlFieldType> argumentTypes;

  private SamzaSqlFieldType returnType;

  public UdfDisplayInfo(String name, String description, List<SamzaSqlFieldType> argumentTypes,
      SamzaSqlFieldType returnType) {
    this.name = name;
    this.description = description;
    this.argumentTypes = argumentTypes;
    this.returnType = returnType;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public List<String> getArgumentTypes() {
    return argumentTypes.stream().map(x -> x.getTypeName().toString()).collect(Collectors.toList());
  }

  public String getReturnType() {
    return returnType.getTypeName().toString();
  }

  public String toString() {
    List<String> argumentTypeNames =
        argumentTypes.stream().map(x -> x.getTypeName().toString()).collect(Collectors.toList());
    String args = Joiner.on(", ").join(argumentTypeNames);
    return String.format("%s(%s) returns <%s> : %s", name, args, returnType.getTypeName().toString(), description);
  }
}
