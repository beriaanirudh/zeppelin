package org.apache.zeppelin.socket;

/** This is just a holder class
 *  for a pair of zeppelinId and qbolUserId.
 */
public class ZeppelinQbolUserPair {

  private String zeppelinId;
  private String qbolUserId;

  public ZeppelinQbolUserPair(String zeppelinId, String qbolUserId) {
    this.zeppelinId = zeppelinId;
    this.qbolUserId = qbolUserId;
  }

  public String getQbolUserId() {
    return qbolUserId;
  }

  public String getZeppelinId() {
    return zeppelinId;
  }
}
