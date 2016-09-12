package org.apache.zeppelin.s3;

import org.apache.zeppelin.util.QuboleUtil;

/**
 * 
 * Utility class for s3 operations
 *
 */
public class S3Utils {

  static boolean isValidS3Path(String path) {
    boolean valid = true;
    if (path == null || path.length() == 0
        || !(path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://"))) {
      valid = false;
    }
    return valid;
  }

  static String getS3cmdPath() {
    return QuboleUtil.s3cmd;
  }
}
