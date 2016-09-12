package org.apache.zeppelin.s3;

import java.io.IOException;
/**
 * 
 * @author interface for s3 commands
 *
 */

public interface S3Command {

  S3OperationsEnum getOperation();

  String getCommand();

  int execute() throws IOException, InterruptedException;

  String getSource();

  void setSource(String source);

  String getDestination();

  void setDestination(String dest);

  boolean validate();

  boolean isSuccess();
}
