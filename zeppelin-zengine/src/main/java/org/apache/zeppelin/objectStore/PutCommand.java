package org.apache.zeppelin.objectStore;

import org.apache.zeppelin.util.QuboleUtil;

/**
 * 
 * Command for objectStore PUT ops
 *
 */

public class PutCommand extends AbstractWriteCommands {

  @Override
  public ObjectStoreOperationsEnum getOperation() {
    return ObjectStoreOperationsEnum.PUT;
  }

  @Override
  public String getCommand() {
    if (QuboleUtil.useHadoopCmd()) {
      return Utils.getHadoopCmdPath() + " dfs -copyFromLocal -f " +
              getSource() + " " + getDestination();
    }
    else {
      return Utils.getS3cmdPath() + " --no-check-md5 put " + getSource() + " " + getDestination();
    }

  }
}
