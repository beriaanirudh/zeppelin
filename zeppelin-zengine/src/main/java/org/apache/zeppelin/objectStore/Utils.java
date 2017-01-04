package org.apache.zeppelin.objectStore;

import org.apache.zeppelin.util.QuboleUtil;

/**
 * 
 * Utility class for objectStore operations
 *
 */
public class Utils {

  static boolean isValidPath(String path) {
    boolean valid = true;
    if (path == null || path.length() == 0) {
      valid = false;
    }
    return valid;
  }

  static String getS3cmdPath() {
    return QuboleUtil.s3cmd;
  }

  static String getHadoopCmdPath() {
    return QuboleUtil.hadoopcmd;
  }
}
