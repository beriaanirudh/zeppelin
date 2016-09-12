package org.apache.zeppelin.s3;

/**
 * 
 * @author Abstract class for s3 write operations
 *
 */
public abstract class AbstractS3WriteCommands extends AbstractS3Command {

  @Override
  public boolean validate() {
    boolean valid = super.validate();

    valid &= S3Utils.isValidS3Path(getDestination());
    return valid;
  }

}
