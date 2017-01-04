package org.apache.zeppelin.objectStore;
/**
 * 
 * ObjectStore operation types
 *
 */

public enum ObjectStoreOperationsEnum { // using the constructor defined below
  GET("get"), PUT("put"), SYNC("sync");

  // Member to hold the operation name
  private String opName;

  ObjectStoreOperationsEnum(String name) {
    opName = name;
  }

  @Override
  public String toString() {
    return opName;
  }
}
