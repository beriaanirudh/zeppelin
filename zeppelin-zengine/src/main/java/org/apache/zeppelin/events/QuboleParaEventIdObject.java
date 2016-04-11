package org.apache.zeppelin.events;

/**
 * Captures event id related returned by rails When a pargarph execution starts,
 * we make request to tapp to record the event. Tapp returns an event id,
 * captured here. When a paragraph execution ends, this id is returned to tapp.
 * This id is used for identifying the row in zeppelin_events table that was
 * created as a part of paragraph execution start, so that the same row is
 * updated during paragraph execution end.
 */
public class QuboleParaEventIdObject {
  private transient String paraEventId;

  public String getParaEventId() {
    return paraEventId;
  }

  public void setParaEventId(String parEventId) {
    this.paraEventId = parEventId;
  }
}
