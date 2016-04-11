package org.apache.zeppelin.events;

import org.apache.zeppelin.events.QuboleEventsEnum.EVENTTYPE;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.util.QuboleUtil;

/**
 * captures interpreter setting related event
 */
public class QuboleInterpreterSettingEvent {

  private String id;
  private String name;
  private String source;
  private String group;

  public QuboleInterpreterSettingEvent(InterpreterSetting setting, EVENTTYPE event) {
    id = setting.id();
    name = setting.getName();
    source = (String) setting.getProperties().get(QuboleUtil.SOURCE);
    group = setting.getGroup();
  }
}
