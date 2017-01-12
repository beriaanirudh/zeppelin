package org.apache.zeppelin.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
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
  private static final String S3Loc = System.getenv("S3_FIRST_CLASS_NOTEBOOK_LOC");
  private static final String opsApiPath = "/opsapi/v1/zeppelin";
  private static final String s3cmd = "/usr/bin/s3cmd -c /usr/lib/hustler/s3cfg";
  private static final String clusterId = System.getenv("CLUSTER_ID");
  private static final ZeppelinConfiguration zepConfig = ZeppelinConfiguration.create();

  public static final String SOURCE = "source";
  public static final String JOBSERVER = "JobServer";
  public static final String INTERPRETER_SETTINGS = "interpreterSettings";
  public static final String PROPERTIES = "properties";

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

  private static HttpURLConnection sendRequestToQuboleRails(String apiPath,
      Map<String, String> params, String requestMethod) {
    int retries = 4;
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
      } catch (IOException  | InterruptedException e) {
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

  public static void fetchFromS3(String noteId) throws IOException {
    if (FEATURE_DISABLED)
      return;
    String noteBookDir = zepConfig.getNotebookDir();
    String downloadPath = noteBookDir + "/" + noteId + "/note.json";

    String s3DownloadPath = getS3Path(noteId);
    LOG.info("Trying to download note from " + s3DownloadPath + " to " + downloadPath);
    String downloadCommand = s3cmd + " get  " + s3DownloadPath + " " + downloadPath;
    LOG.info("Download command:\n" + downloadCommand);
    try {
      Process process = Runtime.getRuntime().exec(downloadCommand);
      process.waitFor();
    } catch (InterruptedException e) {
      LOG.info("Interrupted Exception occured!");
    }

    File f = new File(downloadPath);
    if (!f.isFile()) {
      throw new IOException("File could not be downloaded from "
          + "S3." + " Please check the directory.");
    }
  }

  private static String getS3Path(String noteId) {
    String s3Loc = S3Loc;
    if (!s3Loc.endsWith("/")) {
      s3Loc = s3Loc + "/";
    }
    return s3Loc + noteId + "/note.json";
  }

  public static void initNoteBook() {
    if (FEATURE_DISABLED) {
      return;
    }
    String apiPath = opsApiPath;
    LOG.info("Making GET request to " + apiPath + " to update notebook");

    try {
      HttpURLConnection connection = sendRequestToQuboleRails(apiPath, null, "GET");
      int responseCode = connection.getResponseCode();
      if (responseCode == 200) {
        InputStream inputStream = connection.getInputStream();
        BufferedReader bis = new BufferedReader(new InputStreamReader(inputStream));
        String readLine = bis.readLine();
        JsonObject obj = (JsonObject) new JsonParser().parse(readLine);
        JsonElement jsonNotes = obj.get("notes");

        JsonElement parse = new JsonParser().parse(jsonNotes.getAsString());
        if (parse instanceof JsonArray) {
          JsonArray arr = (JsonArray) parse;
          for (int i = 0; i < arr.size(); i++) {
            JsonElement jsonElement = arr.get(i);
            String noteId = jsonElement.getAsString();
            fetchFromS3(noteId);
          }
        }
        LOG.info("GET request to rails successful");
      } else {
        LOG.info(responseCode + " error in making opsapi call to rails");
      }
    } catch (IOException e) {
      LOG.info(e.toString());
    }
  }

  public static String getClusterId(){
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
}
