package org.apache.zeppelin.s3;
/**
 * 
 * S3 operation types
 *
 */

public enum S3OperationsEnum { // using the constructor defined below
  GET("get"), PUT("put"), SYNC("sync");

  // Member to hold the operation name
  private String opName;

  S3OperationsEnum(String name) {
    opName = name;
  }

  @Override
  public String toString() {
    return opName;
  }
}
