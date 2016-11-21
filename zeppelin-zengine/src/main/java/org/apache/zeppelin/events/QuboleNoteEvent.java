package org.apache.zeppelin.events;

import java.io.Serializable;

import org.apache.zeppelin.events.QuboleEventsEnum.EVENTTYPE;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.util.QuboleNoteAttributes;

/**
 *
 * Captures note related event
 *
 */
public class QuboleNoteEvent implements Serializable {

  private String name;
  private String zeppelin_id;
  private String source;
  private String id;

  public QuboleNoteEvent(Note note, EVENTTYPE event) {
    name = note.getName();
    zeppelin_id = note.id();
    source = note.getSource();
    QuboleNoteAttributes quboleNoteAttributes = note.getQuboleNoteAttributes();
    if (quboleNoteAttributes != null) {
      Integer noteId = quboleNoteAttributes.getNoteId();
      if (noteId != null) {
        id = noteId.toString();
      }
    }
  }

}
