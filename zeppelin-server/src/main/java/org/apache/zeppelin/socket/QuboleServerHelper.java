package org.apache.zeppelin.socket;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 
 * Qubole Helper 
 */
public class QuboleServerHelper {

  private static final Map<NotebookSocket, String> socketToUserIdMap = new ConcurrentHashMap<>();
  public static final String QBOL_USER_ID = "qboluserid";
  public static final String CRON_EXECUTING_USER = "cronExecutingUser";

  public static void addConnToUserMapping(NotebookSocket conn) {
    String userId = conn.getRequest().getHeader(QBOL_USER_ID);
    if (conn != null && userId != null) {
      socketToUserIdMap.put(conn, userId);
    }
  }

  public static String getUserForConn(NotebookSocket conn) {
    if (conn == null) return null;
    return socketToUserIdMap.get(conn);
  }

}
