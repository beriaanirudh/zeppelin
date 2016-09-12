package org.apache.zeppelin.s3;

import java.io.File;

/**
 *
 * concrete class for s3 get commands
 *
 */
public class S3GetCommand extends AbstractS3Command {

  @Override
  public S3OperationsEnum getOperation() {
    return S3OperationsEnum.GET;
  }

  @Override
  public String getCommand() {
    return S3Utils.getS3cmdPath() + " get --skip-existing " + getSource() + " " + getDestination();
  }

  @Override
  public boolean validate() {
    boolean valid = super.validate();
    if (valid) {
      valid = S3Utils.isValidS3Path(getSource());
    }
    return valid;
  }

  @Override
  public boolean isSuccess() {
    boolean success = false;
    File f = new File(getDestination());
    if (f.exists() && f.length() != 0) {
      success = true;
    } else {
      if (f.length() != 0) {
        f.delete();
      }
    }
    return success;
  }
}
