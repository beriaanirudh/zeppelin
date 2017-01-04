package org.apache.zeppelin.objectStore;

import org.apache.zeppelin.util.QuboleUtil;

/**
 * 
 * command for objectStore sync ops
 *
 */

public class SyncCommand extends AbstractWriteCommands {

  @Override
  public ObjectStoreOperationsEnum getOperation() {
    return ObjectStoreOperationsEnum.SYNC;
  }

  @Override
  public String getCommand() {
    if (QuboleUtil.useHadoopCmd()) {
      return Utils.getHadoopCmdPath() + " dfs -sync " +
              getSource() + " " + getDestination();
    }
    else {
      return Utils.getS3cmdPath() + " --no-check-md5 sync " + getSource() + " " + getDestination();
    }

  }

}
