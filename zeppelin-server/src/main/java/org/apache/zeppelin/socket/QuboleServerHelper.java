package org.apache.zeppelin.socket;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.zeppelin.events.QuboleEventsEnum.EVENTTYPE;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.rest.message.NewParagraphRunRequest;
import org.apache.zeppelin.rest.message.RunNotebookResponse;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.server.ZeppelinServer;
import org.apache.zeppelin.util.QuboleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static  org.apache.zeppelin.socket.QuboleACLHelper.Operation.READ;
import static  org.apache.zeppelin.socket.QuboleACLHelper.Operation.WRITE;

/** 
 * Qubole Helper 
 */
public class QuboleServerHelper {

  private static final Map<NotebookSocket, String> socketToUserIdMap = new ConcurrentHashMap<>();
  private static final Map<String, String> userIdToEmailMap = new ConcurrentHashMap<>();
  public static final String QBOL_USER_ID = "qboluserid";
  public static final String QBOL_USER_EMAIL = "qboluseremail";
  public static final String CRON_EXECUTING_USER = "cronExecutingUser";
  private static final String HEADER_FETCH_KEY = "Qubole-Operation";
  private static final String COMMIT_OPERATION = "commit";
  private static final String PARA_STATUS_KEY = "status";

  private static final Logger LOG = LoggerFactory.getLogger(QuboleServerHelper.class);

  public static void addConnToUserMapping(NotebookSocket conn) {
    String userId = conn.getRequest().getHeader(QBOL_USER_ID);
    if (conn != null && userId != null) {
      socketToUserIdMap.put(conn, userId);
    }

    String email = conn.getRequest().getHeader(QBOL_USER_EMAIL);
    if (conn != null && email != null) {
      userIdToEmailMap.put(userId, email);
    }
    QuboleUtil.putUserForEmail(email, userId);
    QuboleUtil.putEmailForUser(userId, email);
  }

  public static String getUserForConn(NotebookSocket conn) {
    if (conn == null) return null;
    return socketToUserIdMap.get(conn);
  }

  /**
   * Fetch the note for commit / import. This API is
   * similar to getNote(), the differences are:
   * 1. Syncs note to S3.
   * 2. Uses Gson's pretty printing to write files.
   * 3. Checks for valid state before commit.
   */
  public static Response fetch(HttpServletRequest request,
      Notebook notebook, String noteId) {

    if (!QuboleACLHelper.isOperationAllowed(noteId, request, notebook, READ)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }
    String denyCommitMessage = noteValidForCommit(request, notebook, noteId);

    if (denyCommitMessage != null) {
      return new JsonResponse<>(Status.FORBIDDEN, denyCommitMessage).build();
    }
    Note note = notebook.getNote(noteId);
    if (note == null) {
      LOG.error("Note=" + noteId + " not found.");
      return new JsonResponse<>(Status.NOT_FOUND, "Note not found").build();
    }
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    QuboleUtil.syncNotesToFolders();
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
  public static Response checkout(Notebook notebook, String noteId,
                                  String data, HttpServletRequest request) {
    if (!QuboleACLHelper.isOperationAllowed(noteId, request, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

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
      return new JsonResponse<>(Status.BAD_REQUEST,
          "Checked-out file has invalid json").build();
    }

    String notebookDir = QuboleUtil.getNotebookDir();
    String fileName = notebookDir + (notebookDir.endsWith("/") ? "" : "/")
        + noteId + "/note.json";

    try {
      PrintWriter writer = new PrintWriter(fileName);
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
      QuboleUtil.syncNotesToFolders();
    } catch (IOException e) {
      LOG.error("Exception while checking out note: " + e.getMessage());
      return new JsonResponse<>(Status.NOT_FOUND, e.getMessage()).build();
    }

    LOG.info("Checked out version from github for note = " + noteId);
    return new JsonResponse<>(Status.OK).build();

  }

  /**
   * This API is called by spark-sql driver to
   * send qlog. The qlog is sent to middleware via
   * 'events' API. This is done in addition
   * because we want qlog present in middleware as
   * soon as possible.
   */
  public static Response receiveAndSendQlog(String req, Notebook notebook) {
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = (JsonObject) jsonParser.parse(req);

    String noteId = jsonObject.get("noteId").getAsString();
    String paragraphId = jsonObject.get("paragraphId").getAsString();
    Note note = notebook.getNote(noteId);
    Paragraph paragraph = note.getParagraph(paragraphId);
    Integer queryHistId = paragraph.getQueryHistId();
    if (queryHistId != null) {
      JsonObject sendJson = new JsonObject();
      sendJson.addProperty("event_type", EVENTTYPE.SAVE_QLOG.toString());
      sendJson.add("qlog", jsonObject.get("qlog"));
      sendJson.addProperty("query_hist_id", queryHistId);

      LOG.info("Sending qlog for queryHistId = " + queryHistId + " and paragraphId = "
          + paragraphId + " to middleware in async thread");
      QuboleUtil.sendEventAsync(sendJson.toString(), 4);
    }
    return new JsonResponse<>(Status.OK).build();
  }

  public static void setQueryHistInParagraph(NewParagraphRunRequest request, Paragraph p,
      String noteId) {
    if (request.getQueryHistId() != null) {
      LOG.info("Storing queryHistId = " + request.getQueryHistId() +
          " in paragraph = " + p.getId() + " of note = " + noteId);
      p.setQueryHistId(request.getQueryHistId());
    }
  }

  public static Response conformResponseForTapp(Note note) {
    List<Map<String, String>> paraInfos = note.generateParagraphsInfo();
    Job.Status status = Job.Status.RUNNING;
    Integer finishCount = 0;
    for (Map<String, String> pInfo : paraInfos) {
      String pStatus = pInfo.get(PARA_STATUS_KEY);
      if (Job.Status.ERROR.toString().equals(pStatus)) {
        status = Job.Status.ERROR;
        finishCount += 1;
      }
      if (Job.Status.ABORT.toString().equals(pStatus)) {
        status = Job.Status.ABORT;
        finishCount += 1;
      }
      if (Job.Status.FINISHED.toString().equals(pStatus)) {
        finishCount += 1;
      }
    }
    
    if (finishCount == paraInfos.size() && Job.Status.RUNNING.equals(status)) {
      status = Job.Status.FINISHED;
    }
    else if (finishCount != paraInfos.size()) {
      status = Job.Status.RUNNING;
    }
    
    RunNotebookResponse runNotebookResponse = new RunNotebookResponse(status.toString(), paraInfos);
    return new JsonResponse<>(Status.OK, null, runNotebookResponse).build();
  }

  public static void setEmailForParagraph(Paragraph p, NotebookSocket conn) {
    String email = null;
    String userId = null;
    if (conn != null) {
      userId = socketToUserIdMap.get(conn);
      if (userId != null) {
        email = userIdToEmailMap.get(userId);
      }
    }
    if (email == null) {
      LOG.warn("Could not set Email for paragraph, conn = {} and user={}", conn, userId);
      return;
    }
    p.setUser(email);
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

  private static String noteValidForCommit(HttpServletRequest request,
      Notebook notebook, String noteId) {

    if (!COMMIT_OPERATION.equals(request.getHeader(HEADER_FETCH_KEY))) {
      return null;
    }

    if (isNoteRunning(notebook, noteId)) {
      return "Cannot commit while notebook is running";
    }

    return null;
  }

}
