package org.apache.zeppelin.s3;

/**
 * 
 * command for s3 sync ops
 *
 */

public class S3SyncCommand extends AbstractS3WriteCommands {

  @Override
  public S3OperationsEnum getOperation() {
    return S3OperationsEnum.SYNC;
  }

  @Override
  public String getCommand() {
    return S3Utils.getS3cmdPath() + " --no-check-md5 sync " + getSource() + " " + getDestination();
  }

}
