package org.apache.zeppelin.objectStore;

import java.io.IOException;
/**
 * 
 * @author interface for ObjectStore commands
 *
 */

public interface Command {

  ObjectStoreOperationsEnum getOperation();

  String getCommand();

  int execute() throws IOException, InterruptedException;

  String getSource();

  void setSource(String source);

  String getDestination();

  void setDestination(String dest);

  boolean validate();

  boolean isSuccess();
}
