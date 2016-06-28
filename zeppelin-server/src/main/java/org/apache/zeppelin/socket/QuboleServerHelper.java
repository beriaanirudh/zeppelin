package org.apache.zeppelin.socket;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.server.ZeppelinServer;
import org.apache.zeppelin.util.QuboleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/** 
 * Qubole Helper 
 */
public class QuboleServerHelper {

  private static final Map<NotebookSocket, String> socketToUserIdMap = new ConcurrentHashMap<>();
  public static final String QBOL_USER_ID = "qboluserid";
  public static final String CRON_EXECUTING_USER = "cronExecutingUser";
  private static final Logger LOG = LoggerFactory.getLogger(QuboleServerHelper.class);

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

  /**
   * Fetch the note for commiting
   * to github. This API is similar to
   * getNote(). This API is used to be
   * consistent in using Gson's pretty
   * printing like checkout().
   */
  public static Response fetchForCommit(Notebook notebook, String noteId) {
    if (isNoteRunning(notebook, noteId)) {
      return new JsonResponse<>(Status.FORBIDDEN,
          "Cannot commit while notebook is running").build();
    }
    Note note = notebook.getNote(noteId);
    if (note == null) {
      LOG.error("Note=" + noteId + " not found while fetching for commit");
      return new JsonResponse<>(Status.NOT_FOUND, "Note not found").build();
    }
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    QuboleUtil.syncNotesToS3();
    LOG.info("Returing latest note = " + noteId);
    return new JsonResponse<>(Status.OK, gson.toJson(note)).build();
  }

  /**
   * Replace the note.json for this
   * note with specified file content.
   * It then reloads the note, and broadcasts
   * this to all connected users, so that
   * all notes are updated to the latest content.
   */
  public static Response checkout(Notebook notebook, String noteId, String data) {
    if (isNoteRunning(notebook, noteId)) {
      return new JsonResponse<>(Status.FORBIDDEN,
          "Cannot re-store while notebook is running").build();
    }

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    Map<Object, Object> noteMap;

    try {
      Map<Object, Object> reqMap = (Map<Object, Object>) gson.fromJson(data, Map.class);
      noteMap = (Map<Object, Object>) gson.fromJson(reqMap.get("note_str").toString(), Map.class);

      /* Zeppelin loads the note.json file as a JSON
       * and checks the 'id' field and considers that
       * as the source of truth. In order to be consistent
       * with rails 'note_id' and also the directory where
       * note.json is stored in zeppelin, the id written
       * in note.json needs to be replaced with proper id.
       */
      noteMap.put("id", noteId);
      Map<Object, Object> confMap = (Map<Object, Object>) noteMap.get("config");

      /* 1. Keeping the cron job intact.
       * 2. Editing the cron updater to the owner of the note
       */
      String noteOwner = (String) gson.fromJson(reqMap.get("note_owner").toString(), String.class);
      if (confMap != null && confMap.containsKey("cron")) {
        confMap.put("cron_updated_by_userid", noteOwner);
      }

    } catch (Exception e) {
      LOG.error("Error while parsing json during checkout: " + e.getMessage());
      return new JsonResponse<>(Status.BAD_REQUEST).build();
    }

    String notebookDir = QuboleUtil.getNotebookDir();
    String fileName = notebookDir + (notebookDir.endsWith("/") ? "" : "/")
        + noteId + "/note.json";

    try {
      PrintWriter writer = new PrintWriter("../" + fileName);
      writer.print(gson.toJson(noteMap));
      writer.close();
    } catch (FileNotFoundException e) {
      LOG.error("Error while writing note.json during checkout "
          + e.getMessage());
      return new JsonResponse<>(Status.NOT_FOUND,
          "note.json not found").build();
    }

    try {
      Note note = notebook.loadNoteFromRepo(noteId, null);
      ZeppelinServer.notebookWsServer.broadcastNote(note, true);
      QuboleUtil.syncNotesToS3();
    } catch (IOException e) {
      LOG.error("Exception while checking out note: " + e.getMessage());
      return new JsonResponse<>(Status.NOT_FOUND, e.getMessage()).build();
    }

    LOG.info("Checked out version from github for note = " + noteId);
    return new JsonResponse<>(Status.OK).build();

  }

  private static boolean isNoteRunning(Notebook notebook, String noteId) {
    Note note = notebook.getNote(noteId);
    for (Paragraph paragraph: note.getParagraphs()) {
      if (paragraph.isRunning() || paragraph.isPending()) {
        return true;
      }
    }
    return false;
  }

}
