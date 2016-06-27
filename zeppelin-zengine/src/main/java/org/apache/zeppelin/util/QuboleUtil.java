package org.apache.zeppelin.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Utility methods for communication with Qubole web tier
 */

public class QuboleUtil {

  private static final Logger LOG = LoggerFactory.getLogger(QuboleUtil.class);
  private static final String quboleBaseURL = System.getenv("QUBOLE_BASE_URL");
  private static final String quboleApiToken = System.getenv("QUBOLE_API_TOKEN");
  private static boolean FEATURE_DISABLED = false; // expose this as env
                                                   // variable??
  private static final String S3Loc = System.getenv("FIRST_CLASS_NOTEBOOK_LOC");
  private static final String opsApiPath = "/opsapi/v1/zeppelin";
  private static final String s3cmd = "/usr/bin/s3cmd -c /usr/lib/hustler/s3cfg";
  private static final String clusterId = System.getenv("CLUSTER_ID");
  private static final ZeppelinConfiguration zepConfig = ZeppelinConfiguration.create();

  public static final String SOURCE = "source";
  public static final String JOBSERVER = "JobServer";
  public static final String INTERPRETER_SETTINGS = "interpreterSettings";
  public static final String PROPERTIES = "properties";
  private static final ExecutorService s3Executor = Executors.newFixedThreadPool(5);

  /**
   * make opsapi call to qubole rails tier to convey creation of new note
   **/
  public static void updateNewNoteInRails(Note note, String source) {
    if (FEATURE_DISABLED) {
      return;
    }
    Map<String, String> params = new HashMap<String, String>();
    params.put("name", note.getName());
    params.put("note_id", note.id());
    params.put("source", source);

    String apiPath = opsApiPath;
    LOG.info("Making POST request to " + apiPath + " to add new note");
    sendRequestToQuboleRails(apiPath, params, "POST");
  }

  /**
   * make opsapi call to qubole rails tier to convey deletion of zeppelin note
   */
  public static void updateNoteDeletionInRails(String noteId) {
    if (FEATURE_DISABLED) {
      return;
    }
    String apiPath = opsApiPath + "/" + noteId;
    LOG.info("Making DELETE request to " + apiPath + " to delete note");
    sendRequestToQuboleRails(apiPath, null, "DELETE");
  }

  /**
   * update name change of note in rails
   */
  public static void updateNoteNameChangeInRails(Note note) {
    if (FEATURE_DISABLED) {
      return;
    }
    String apiPath = opsApiPath + "/" + note.id();
    Map<String, String> params = new HashMap<String, String>();
    params.put("name", note.getName());
    sendRequestToQuboleRails(apiPath, params, "PUT");
  }

  /**
   * send events to web tier
   */
  public static HttpURLConnection sendEvent(String event) {
    String apiPath = opsApiPath + "/events/";
    Map<String, String> params = new HashMap<String, String>();
    params.put("event", event);
    return sendRequestToQuboleRails(apiPath, params, "POST", 1);
  }

  private static HttpURLConnection sendRequestToQuboleRails(String apiPath,
      Map<String, String> params, String requestMethod) {
    return sendRequestToQuboleRails(apiPath, params, requestMethod, 4);
  }

  private static HttpURLConnection sendRequestToQuboleRails(String apiPath,
      Map<String, String> params, String requestMethod, int numRetries) {
    if (numRetries <= 0)
      return null;
    int retries = numRetries;
    while (retries > 0) {
      try {
        HttpURLConnection connection = (HttpURLConnection) (new URL(getQuboleBaseURL() + apiPath))
            .openConnection();
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("X-AUTH-TOKEN", getQuboleApiToken());
        connection.setRequestMethod(requestMethod);
        connection.setDoOutput(true);
        if (params != null) {
          Gson gson = new Gson();
          String jsonData = gson.toJson(params);
          OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
          out.write(jsonData);
          out.flush();
          out.close();
        }
        int responseCode = connection.getResponseCode();
        if (responseCode == 200) {
          LOG.info(requestMethod + " request to rails successful");
          return connection;
        } else {
          LOG.info(responseCode + " error in making opsapi call to rails");
          LOG.info("Waiting for 5 seconds");
          Thread.sleep(5000);
        }
      } catch (IOException | InterruptedException e) {
        LOG.info(e.toString());
      }
      retries--;
    }
    return null;
  }

  private static String getQuboleApiToken() {
    LOG.info("Api token is: " + quboleApiToken);
    return quboleApiToken;
  }

  private static String getQuboleBaseURL() {
    LOG.info("Api Base is: " + quboleBaseURL);
    return quboleBaseURL;
  }

  public static void fetchFromS3(List<String> noteIds) throws IOException {
    if (noteIds.isEmpty()) {
      LOG.info("No notes for this cluster");
      return;
    }
    File includesFile = createTempFileWithNoteIds(noteIds);
    File tempDownloadDir = createTempDownloadDirectory();

    if (!tempDownloadDir.exists()) {
      return;
    }
    String s3DownloadPath = S3Loc.endsWith("/") ? S3Loc : S3Loc + "/";
    String downloadCmd = s3cmd + " get -r --skip-existing --rinclude-from="
        + includesFile.getAbsolutePath() + " --rexclude=.*  " + s3DownloadPath + " "
        + tempDownloadDir.getAbsolutePath();
    int numRetries = 4;
    for (int i = 0; i < numRetries; i++) {
      try {
        Process process = Runtime.getRuntime().exec(downloadCmd);
        process.waitFor();
        cleanNotesDownloadDirectory(tempDownloadDir);
        if (tempDownloadDir.listFiles().length == noteIds.size()) {
          break;
        }
        LOG.info("Partial download occurred. Retrying download of notes");
      } catch (InterruptedException e) {
        LOG.info("Exception occured when trying to download notes" + e.getMessage());
      }
      try {
        Thread.sleep(500);
        LOG.info("Retrying download of notes");
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    }
    String noteBookDir = zepConfig.getNotebookDir();
    noteBookDir = noteBookDir.endsWith("/") ? noteBookDir.substring(0, noteBookDir.length() - 1)
        : noteBookDir;
    copyNotesToNoteBookDir(new File(noteBookDir), tempDownloadDir);
    includesFile.delete();
  }

  private static File createTempFileWithNoteIds(List<String> noteIds) throws IOException {
    File includesFile = File.createTempFile("noteincludes", null);
    PrintWriter writer = new PrintWriter(includesFile);
    for (String noteId : noteIds) {
      writer.println("^" + noteId + "/");
    }
    writer.close();
    return includesFile;
  }

  private static File createTempDownloadDirectory() throws IOException {
    File tempDownloadDir = new File("/tmp/tempnotesloc");
    if (tempDownloadDir.exists()) {
      FileUtils.deleteDirectory(tempDownloadDir);
    }
    if (!tempDownloadDir.mkdir()) {
      LOG.error(
          "Error while creating directory to download Path:" + tempDownloadDir.getAbsolutePath());
      throw new IOException("Error occured when creating temp directory for download");
    }
    return tempDownloadDir;
  }

  private static void copyNotesToNoteBookDir(File notebookDir, File downloadDir)
      throws IOException {
    LOG.info("Copy notes to notebook directory");
    List<String> notes = new ArrayList<>();
    if (!notebookDir.exists()) {
      LOG.info("notebookdir doesnot exist");
      return;
    }
    File[] availableNotes = notebookDir.listFiles();
    for (int i = 0; i < availableNotes.length; i++) {
      notes.add(availableNotes[i].getName());
    }
    File[] files = downloadDir.listFiles();
    if (files != null && files.length > 0) {
      for (File aFile : files) {
        try {
          if (notes.contains(aFile.getName())) {
            LOG.info("Note already present id :" + aFile.getName());
            continue;
          }
          FileUtils.copyDirectoryToDirectory(aFile, notebookDir);
          LOG.info("Note download sucessful id: " + aFile.getName());

        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }
  }

  private static boolean hasEmptyNote(File file) {
    boolean empty = true;
    if (file.isDirectory()) {
      File[] listFiles = file.listFiles();
      if (listFiles.length == 1) {
        File noteFile = listFiles[0];
        empty = !(noteFile.getName().equals("note.json") && noteFile.length() > 0);
        if (empty) {
          LOG.warn("Note not downloaded for note: " + file.getName());
        }
      }
    }
    return empty;
  }

  private static void cleanNotesDownloadDirectory(File dir) throws IOException {
    File[] notes = dir.listFiles();
    for (File notedir : notes) {
      if (hasEmptyNote(notedir)) {
        FileUtils.deleteDirectory(notedir);
      }
    }
  }

  public static void initNoteBook() throws IOException {
    if (FEATURE_DISABLED) {
      return;
    }
    String apiPath = opsApiPath;
    LOG.info("Making GET request to " + apiPath + " to update notebook");

    HttpURLConnection connection = sendRequestToQuboleRails(apiPath, null, "GET");
    int responseCode = connection.getResponseCode();
    if (responseCode == 200) {
      LOG.info("GET request to rails successful");
      InputStream inputStream = connection.getInputStream();
      BufferedReader bis = new BufferedReader(new InputStreamReader(inputStream));
      String readLine = bis.readLine();
      JsonObject obj = (JsonObject) new JsonParser().parse(readLine);
      JsonElement jsonNotes = obj.get("notes");

      JsonElement parse = new JsonParser().parse(jsonNotes.getAsString());
      List<String> noteIds = new ArrayList<>();
      if (parse instanceof JsonArray) {
        JsonArray arr = (JsonArray) parse;
        for (int i = 0; i < arr.size(); i++) {
          JsonElement jsonElement = arr.get(i);
          String noteId = jsonElement.getAsString();
          noteIds.add(noteId);
        }
      }
      fetchFromS3(noteIds);
    } else {
      LOG.info(responseCode + " error in making opsapi call to rails");
    }
  }

  public static String getClusterId() {
    return clusterId;
  }

  public static void addJobServerPropertyToInterpreter(String source, Properties p) {
    if (JOBSERVER.equals(source)) {
      p.put(SOURCE, JOBSERVER);
    }
  }

  public static String removeJobServerInterpreter(String jsonString) {
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.setPrettyPrinting();
    Gson gson = gsonBuilder.create();

    Map<String, Object> infoMap = gson.fromJson(jsonString, Map.class);
    Map<String, Object> settingsMap = (Map<String, Object>) infoMap.get(INTERPRETER_SETTINGS);
    Iterator<Map.Entry<String, Object>> iter = settingsMap.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, Object> entry = iter.next();
      Map<String, Object> interpreterSetting = (Map<String, Object>) entry.getValue();
      Map<String, Object> properties = (Map<String, Object>) interpreterSetting.get(PROPERTIES);
      if (JOBSERVER.equals(properties.get(SOURCE))) {
        iter.remove();
      }
    }
    return gson.toJson(infoMap);
  }

  public static Note downloadNoteIfNull(Notebook notebook, Note note, String noteId) {
    if (note == null) {
      // The note download might have failed for some reason
      // Try to download note from s3 and add here.
      // this can be triggered only from FCN UI
      try {
        note = notebook.fetchAndLoadNoteFromS3(noteId);
      } catch (IOException e) {
        LOG.error("Error while fetching and loading note", e);
      }
      if (note == null) {
        LOG.error("Note download failed" + noteId);
      }
    }
    return note;
  }

  public static String getNotebookDir() {
    return zepConfig.getNotebookDir();
  }

  public static void syncNotesToS3() {
    try {
      s3Executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            Process process = Runtime.getRuntime().exec(
                "/usr/lib/zeppelin/hustler/sync-notes-concurrent.sh");
            process.waitFor();
          } catch (IOException | InterruptedException e) {
            LOG.error("Error while syncing notes to S3: " + e.getMessage());
          }
        }
      });
    } catch (RejectedExecutionException | NullPointerException e) {
      LOG.info("Syncing notes to S3 failed: " + e.getMessage());
    }
  }
}
