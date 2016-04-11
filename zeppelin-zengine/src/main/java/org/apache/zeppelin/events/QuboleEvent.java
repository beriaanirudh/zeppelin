package org.apache.zeppelin.events;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.zeppelin.events.QuboleEventsEnum.EVENTTYPE;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;

/**
 * Root class which captures all events This class is serialized to json and
 * sent to tapp
 */
public class QuboleEvent implements Serializable {

  private QuboleNoteEvent note;
  private QuboleParagraphEvent para;
  private QuboleInterpreterSettingEvent setting;
  private EVENTTYPE event_type;
  private String qbol_user_id;
  private long event_time_ms;
  private String event_id;
  private String cluster_id = System.getenv("CLUSTER_ID");
  private HashMap<Object, Object> infos;

  public QuboleEvent(EVENTTYPE event, String qbolUserId) {
    this.event_type = event;
    this.qbol_user_id = qbolUserId;
    this.event_time_ms = System.currentTimeMillis();
  }

  public void setObject(Object obj) {
    if (obj instanceof Note) {
      createNoteEvent((Note) obj);
    } else if (obj instanceof Paragraph) {
      createParaEvent((Paragraph) obj);
    } else if (obj instanceof InterpreterSetting) {
      createInterpretersettingEvent((InterpreterSetting) obj);
    }
  }

  private void createNoteEvent(Note obj) {
    note = new QuboleNoteEvent(obj, this.event_type);
  }

  private void createParaEvent(Paragraph obj) {
    note = new QuboleNoteEvent(obj.getNote(), this.event_type);
    para = new QuboleParagraphEvent(obj, this.event_type);
  }

  private void createInterpretersettingEvent(InterpreterSetting obj) {
    setting = new QuboleInterpreterSettingEvent(obj, this.event_type);
  }

  public QuboleNoteEvent getNote() {
    return note;
  }

  public QuboleParagraphEvent getParagraph() {
    return para;
  }

  public QuboleInterpreterSettingEvent getInterpreterSetting() {
    return setting;
  }

  public String getQbolUserId() {
    return qbol_user_id;
  }

  public long getEventTime() {
    return event_time_ms;
  }

  public void setInfos(HashMap<Object, Object> infos) {
    this.infos = infos;
  }

  public String getEvent_id() {
    return event_id;
  }

  public void setEvent_id(String event_id) {
    this.event_id = event_id;
  }

}
