package org.apache.zeppelin.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.notebook.Note;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * This class contains meta-data / attributes
 * of notebooks, which are received at zeppelin
 * from Qubole TAPP layer. Note object contains
 * an instance of this class.
 */
public class QuboleNoteAttributes {

  private static final Logger LOG = LoggerFactory.getLogger(QuboleNoteAttributes.class);

  // Per-note attribute which is used as the key for storing the
  // rest of the attributes in the initial response from tapp (which
  // contains all the notebooks).
  private static final String ZEPPELIN_NOTE_ID = "zeppelin_note_id";

  // json field which is per-note and contains rest of the attributes.
  private static final String RESULT_NOTE_ATTRIBUTES = "note_attributes";

  // Per-note attributes.
  private static final String NOTE_OWNER_USER_ID = "owner_user_id";
  private static final String NOTE_OWNER_EMAIL = "owner_email";
  private static final String NOTE_ID = "note_id";
  public static final String LOCATION = "location";

  private String ownerUserId;
  private String ownerEmail;
  private Integer noteId;
  private String location;

  public QuboleNoteAttributes(String ownerUserId,
      String ownerEmail, Integer noteId, String location) {
    this.ownerUserId = ownerUserId;
    this.ownerEmail = ownerEmail;
    this.noteId = noteId;
    this.location = location;
  }

  public String getOwnerUserId() {
    return ownerUserId;
  }

  public String getOwnerEmail() {
    return ownerEmail;
  }

  public Integer getNoteId() {
    return noteId;
  }

  public void setOwnerUserId(String ownerUserId) {
    this.ownerUserId = ownerUserId;
  }

  public void setOwnerEmail(String ownerEmail) {
    this.ownerEmail = ownerEmail;
  }

  public void setNoteId(Integer noteId) {
    this.noteId = noteId;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public static void addNoteAttributesFromConnection(HttpURLConnection connection, Note note) {
    try {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
          connection.getInputStream()));
      String result = bufferedReader.readLine();

      addNoteAttributesFromJson(result, note);
    } catch (IOException e) {
      LOG.error("Error while adding attributes to notes: " + e.getMessage());
    }
  }

  public static Map<String, String> getNoteAttributesFromJSON(String attributesJson) {
    Gson gson = new Gson();
    Map<String, Object> resultMap = gson.fromJson(attributesJson, Map.class);
    Map<String, String> noteAttributes = (Map<String, String>)
        resultMap.get(RESULT_NOTE_ATTRIBUTES);
    return noteAttributes;
  }

  public static void addNoteAttributesFromJson(Map<String, String> noteAttributes, Note note) {
    setNoteAttributes(note, noteAttributes);
  }

  public static void addNoteAttributesFromJson(String attributesJson, Note note) {
    Map<String, String> noteAttributes = getNoteAttributesFromJSON(attributesJson);
    setNoteAttributes(note, noteAttributes);
  }

  public static void setNoteAttributes(Note note, Map<String, String> attributes) {
    if (note == null || attributes == null) return;

    String noteId = attributes.get(NOTE_ID);
    Integer idNote = noteId == null ? null :  new Integer(noteId);
    note.setQuboleNoteAttributes(new QuboleNoteAttributes(
        attributes.get(NOTE_OWNER_USER_ID),
        attributes.get(NOTE_OWNER_EMAIL),
        idNote,
        attributes.get(LOCATION)));
    LOG.info("Setting note attributes for note = " + note.getId() +
        ": " + note.getQuboleNoteAttributes().toString());
  }

  public static Map<String, Map<String, String> > extractAttributes(String request) {
    Map<String, Map<String, String> > notesAttributes = null;
    Gson gson = new Gson();
    Map<String, Object > indexMap = gson.fromJson(request, Map.class);

    notesAttributes = new HashMap<>();
    List<Map<String, String> > indexList = (List<Map<String, String> >)
        gson.fromJson(indexMap.get("notes").toString(), List.class);

    for (Map<String, String> note: indexList) {
      String noteZeppelinId = note.get(ZEPPELIN_NOTE_ID);
      Map<String, String> attribute = new HashMap<>();

      attribute.put(NOTE_OWNER_USER_ID, note.get(NOTE_OWNER_USER_ID));
      attribute.put(NOTE_OWNER_EMAIL, note.get(NOTE_OWNER_EMAIL));
      attribute.put(NOTE_ID, note.get(NOTE_ID));
      attribute.put(LOCATION, note.get(LOCATION));

      notesAttributes.put(noteZeppelinId, attribute);
    }
    return notesAttributes;
  }

  @Override
  public String toString() {
    return "ownerUserId = " + this.ownerUserId +
         "; ownerEmail = " + this.ownerEmail +
         "; noteId = " + this.noteId;
  }
}
