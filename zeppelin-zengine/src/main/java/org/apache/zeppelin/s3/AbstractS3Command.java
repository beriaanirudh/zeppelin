package org.apache.zeppelin.s3;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Abstract class for all s3 operations
 *
 */

public abstract class AbstractS3Command implements S3Command {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractS3Command.class);
  private String source;
  private String destination;

  @Override
  public String getSource() {
    return source;
  }

  @Override
  public void setSource(String source) {
    this.source = source;
  }

  @Override
  public String getDestination() {
    return destination;
  }

  @Override
  public void setDestination(String dest) {
    this.destination = dest;

  }

  @Override
  public boolean validate() {
    return (getSource() != null && getSource().length() != 0 && getDestination() != null
        && getDestination().length() != 0);
  }

  @Override
  public int execute() throws IOException, InterruptedException {
    if (!validate()) {
      throw (new RuntimeException("Validation failed"));
    }
    int numRetries = 2;
    int exitValue = -1;
    while (numRetries > 0) {
      String command = getCommand();
      Process process = Runtime.getRuntime().exec(command);
      process.waitFor();
      exitValue = process.exitValue();
      if (exitValue == 0 && isSuccess()) {
        LOG.debug("s3ops " + getOperation() + " Operation succeeded. Command: " + command);
        break;
      }
      LOG.error("s3ops S3 operation failed command: " + command);
      Thread.sleep(5000);
      numRetries--;
    }
    return exitValue;
  }

  @Override
  public boolean isSuccess() {
    return true;
  }
}
