package org.apache.zeppelin.s3;
/**
 * 
 * Command for s3 PUT ops
 *
 */

public class S3PutCommand extends AbstractS3WriteCommands {

  @Override
  public S3OperationsEnum getOperation() {
    return S3OperationsEnum.PUT;
  }

  @Override
  public String getCommand() {
    return S3Utils.getS3cmdPath() + " --no-check-md5 put " + getSource() + " " + getDestination();
  }

}
