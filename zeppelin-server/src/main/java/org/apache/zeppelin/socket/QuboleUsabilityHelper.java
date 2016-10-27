package org.apache.zeppelin.socket;


import java.io.StringReader;
import java.util.Map;

import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.StringMap;
import com.google.gson.stream.JsonReader;

public class QuboleUsabilityHelper {
  private static final String PARAGRAPH_OUTPUT_LIMIT_STR = 
      System.getenv("ZEPPELIN_OUTPUT_SIZE_LIMIT");
  //Outputs larger than OUTPUT_LIMIT in bytes will not be sent during GET_NOTE call.
  private static final Integer PARAGRAPH_OUTPUT_LIMIT =
      (PARAGRAPH_OUTPUT_LIMIT_STR == null || PARAGRAPH_OUTPUT_LIMIT_STR == "") ? 
      0 : Integer.parseInt(PARAGRAPH_OUTPUT_LIMIT_STR);
  private static final String PARA_OUTPUT_KEY = "msg";
  private static final String PARA_OUTPUT_MESSAGE = "Output exceeds size limit of " +
      PARAGRAPH_OUTPUT_LIMIT_STR + " bytes as configured for this cluster";
  private static final String PARA_TYPE_KEY = "type";
  private static final String PARA_TYPE_TEXT = "TEXT";
  private static final String PARA_OUTPUT_REMOVED_FLAG = "outputRemoved";


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
