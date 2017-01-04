package org.apache.zeppelin.objectStore;

import org.apache.zeppelin.util.QuboleUtil;

import java.io.File;

/**
 *
 * concrete class for ObjectStore get commands
 *
 */
public class GetCommand extends AbstractCommand {

  @Override
  public ObjectStoreOperationsEnum getOperation() {
    return ObjectStoreOperationsEnum.GET;
  }

  @Override
  public String getCommand() {
    if (QuboleUtil.useHadoopCmd()) {
      return Utils.getHadoopCmdPath() + " dfs -copyToLocal -f " + getSource()
              + " " + getDestination();
    }
    else {
      return Utils.getS3cmdPath() + " get --skip-existing " + getSource() + " " + getDestination();
    }
  }

  @Override
  public boolean validate() {
    boolean valid = super.validate();
    if (valid) {
      valid = Utils.isValidPath(getSource());
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
