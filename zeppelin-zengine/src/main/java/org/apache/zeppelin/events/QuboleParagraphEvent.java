package org.apache.zeppelin.events;

import java.io.Serializable;

import org.apache.zeppelin.events.QuboleEventsEnum.EVENTTYPE;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.Interpreter.RegisteredInterpreter;
import org.apache.zeppelin.notebook.Paragraph;

/**
 *
 * capture paragraph events
 *
 */
public class QuboleParagraphEvent implements Serializable {

  private String para_id;
  private String para_type;
  private String para_status;

  public QuboleParagraphEvent(Paragraph para, EVENTTYPE event) {
    para_id = para.getId();
    if (event != EVENTTYPE.PARAGRAPH_REMOVE) {
      para_type = getParaType(para);
    }
    para_status = para.getStatus().toString();
  }

  private String getParaType(Paragraph para) {
    String intpName = Paragraph.getRequiredReplName(para.getText());
    Interpreter repl = para.getRepl(intpName);
    if (repl == null) {
      return null;
    }
    String className = repl.getClassName();
    RegisteredInterpreter registeredIntp = Interpreter
        .findRegisteredInterpreterByClassName(className);
    if (registeredIntp == null)
      return null;
    String group = registeredIntp.getGroup();
    String name = registeredIntp.getName();
    if (group == null) {
      return name;
    }
    return group + "." + name;

  }

}
