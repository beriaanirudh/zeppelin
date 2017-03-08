package org.apache.zeppelin.socket;


import java.io.StringReader;
import java.util.Map;

import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.StringMap;
import com.google.gson.stream.JsonReader;

/** 
 * Qubole Helper 
 */
public class QuboleUsabilityHelper {
  private static final String PARAGRAPH_OUTPUT_LIMIT_STR = 
      System.getenv("ZEPPELIN_OUTPUT_SIZE_LIMIT");
  //Outputs larger than OUTPUT_LIMIT in bytes will not be sent during GET_NOTE call.
  private static Integer PARAGRAPH_OUTPUT_LIMIT = 0; //Default is 30720 characters.
  private static final String PARA_OUTPUT_KEY = "msg";
  private static final String PARA_OUTPUT_MESSAGE = "Output exceeds size limit of " +
      PARAGRAPH_OUTPUT_LIMIT_STR + " bytes as configured for this cluster";
  private static final String PARA_TYPE_KEY = "type";
  private static final String PARA_TYPE_TEXT = "TEXT";
  private static final String PARA_OUTPUT_REMOVED_FLAG = "outputRemoved";

  private static final Logger LOG = LoggerFactory.getLogger(QuboleUsabilityHelper.class);

  static {
    try {
      PARAGRAPH_OUTPUT_LIMIT = Integer.parseInt(PARAGRAPH_OUTPUT_LIMIT_STR);
    } catch (NumberFormatException ne) {
      LOG.info("Not able to set output size limit.", ne);
    }
  }

  public static Note getTrimmedNote(Note note) {
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.setPrettyPrinting();
    Gson gson = gsonBuilder.create();
    JsonReader reader = new JsonReader(new StringReader(gson.toJson(note)));
    reader.setLenient(true);
    Note clonedNote = gson.fromJson(reader, Note.class);
    removeLargeOutputsFromNote(clonedNote);
    return clonedNote;
  }

  private static void removeLargeOutputsFromNote(Note note) {
    if (PARAGRAPH_OUTPUT_LIMIT <= 0) {
      return;
    }
    for (Paragraph para: note.getParagraphs()) {
      removeLargeOutput(para);
    }
  }

  private static void removeLargeOutput(Paragraph p) {
    Object resultObj = p.getReturn();
    if (resultObj instanceof StringMap) {
      Map<String, String> resultMap = (StringMap) resultObj;
      if (resultMap.containsKey(PARA_OUTPUT_KEY)) {
        String resultText = (String) resultMap.get(PARA_OUTPUT_KEY);
        if (resultText.length() > PARAGRAPH_OUTPUT_LIMIT) {
          resultMap.put(PARA_OUTPUT_KEY, PARA_OUTPUT_MESSAGE);
          resultMap.put(PARA_TYPE_KEY, PARA_TYPE_TEXT);
          resultMap.put(PARA_OUTPUT_REMOVED_FLAG, Boolean.TRUE.toString());
        }
      }
    }
  }

}
