package org.apache.zeppelin.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.Notebook.CronJob;
import org.apache.zeppelin.objectStore.Command;
import org.apache.zeppelin.objectStore.CommandManager;
import org.apache.zeppelin.objectStore.ObjectStoreOperationsEnum;
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
  private static final String confLoc = System.getenv("INTERPRETER_CONF_LOC");
  private static final String opsApiPath = "/opsapi/v1/zeppelin";
  private static final String opsApiPathV2 = "/opsapi/v2/zeppelin";
  private static final String permissionPath = "/opsapi/v2/list_access_perms";
  private static final String enable_eventbus = System.getenv("ENABLE_EVENTBUS");
  private static final String eventbus_url = System.getenv("EVENTBUS_URL");
  private static final String clusterId = System.getenv("CLUSTER_ID");
  private static final String clusterTag = System.getenv("CLUSTER_TAG");
  private static final ZeppelinConfiguration zepConfig = ZeppelinConfiguration.create();

  public static final String INTERPRETER_SETTINGS = "interpreterSettings";
  public static final String PROPERTIES = "properties";
  private static final ExecutorService executorService =  Executors.newFixedThreadPool(5);
  private static final ExecutorService eventbusExecutor = new ThreadPoolExecutor(5, 5, 0L, 
      TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000, true));
  private static final boolean useHadoopCmd = "true"
      .equalsIgnoreCase(System.getenv("ENABLE_HADOOP_CMD"));
  private static final ExecutorService syncExecutor = Executors.newFixedThreadPool(5);

  public static final String s3cmd = "/usr/bin/s3cmd -c /usr/lib/hustler/s3cfg ";
  public static final String hadoopcmd = "/usr/lib/hadoop2/bin/hadoop";
  public static final String JOBSERVER = "JobServer";
  public static final String SOURCE = "source";

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
  public static void sendEventAsync(final String event, final int numRetries) {
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        QuboleUtil.sendEvent(event, numRetries);
      }
    });
  }

  /**
   * send events to web tier with numRetries # retries
   */
  public static HttpURLConnection sendEvent(String event, int numRetries) {
    String apiPath = opsApiPath + "/events/";
    Map<String, String> params = new HashMap<String, String>();
    params.put("event", event);
    sendRequestToEventbus(params);
    return sendRequestToQuboleRails(apiPath, params, "POST", numRetries);
  }

  private static void sendRequestToEventbus(final Map<String, String> params) {

    if ("true".equals(enable_eventbus)) {
      try {
        eventbusExecutor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              if (params != null) {
                HttpURLConnection connection = (HttpURLConnection) (new URL(eventbus_url))
                    .openConnection();
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setRequestProperty("Accept", "application/json");
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);

                Gson gson = new Gson();

                List<String> messages = new ArrayList<>();
                List<String> keys = new ArrayList<>();

                Map<String, Object> json = new HashMap<String, Object>();

                String jsonData = gson.toJson(params);
                messages.add(jsonData);
                keys.add(clusterTag);

                json.put("auth_token", getQuboleApiToken());
                json.put("topic", "zeppelinmetrics");
                json.put("key", keys);
                json.put("message", messages);

                OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
                String message_body = gson.toJson(json);
                out.write(message_body);
                out.flush();
                out.close();
                int responseCode = connection.getResponseCode();
                if (responseCode == 200) {
                  LOG.debug("POST request to eventbus successful");
                } else {
                  LOG.warn(responseCode + " error in making call to eventbus");
                  LOG.warn(connection.getResponseMessage());
                }
              }
            } catch (IOException e) {
              LOG.error("Error while sending to eventbus", e);
            }
          }
        });
      } catch (RejectedExecutionException e) {
        LOG.error("Spill while sending data to eventbus", e.getMessage()); 
      }
    }
  }

  public static HttpURLConnection getPermissions(List<Map<String, String>> map) {
    Map<String, String> params = new HashMap<>();
    Gson gson = new Gson();
    params.put("json", gson.toJson(map));
    return sendRequestToQuboleRails(permissionPath, params, "GET", 1);
  }

  /**
   * send events to web tier
   */
  public static HttpURLConnection sendEvent(String event) {
    return sendEvent(event, 1);
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
        if (retries != numRetries) {
          LOG.info("Waiting for 5 seconds");
          Thread.sleep(5000);
        }
        URIBuilder uriBuilder = new URIBuilder(getQuboleBaseURL() + apiPath);
        if ("GET".equalsIgnoreCase(requestMethod) && params != null) {
          for (String key: params.keySet()) {
            uriBuilder.addParameter(key, params.get(key));
          }
        }
        URL url = (URL) uriBuilder.build().toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("X-AUTH-TOKEN", getQuboleApiToken());
        connection.setRequestMethod(requestMethod);
        connection.setDoOutput(true);
        if (!"GET".equalsIgnoreCase(requestMethod) && params != null) {
          Gson gson = new Gson();
          String jsonData = gson.toJson(params);
          OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
          out.write(jsonData);
          out.flush();
          out.close();
        }
        int responseCode = connection.getResponseCode();
        if (responseCode == 200) {
          LOG.debug(requestMethod + " request to rails successful");
          return connection;
        } else {
          LOG.info(responseCode + " error in making opsapi call to rails");
        }
        return connection;
      } catch (IOException | InterruptedException | URISyntaxException e) {
        LOG.info(e.toString());
      }
      retries--;
    }
    return null;
  }

  private static String getQuboleApiToken() {
    LOG.debug("Api token is: " + quboleApiToken);
    return quboleApiToken;
  }

  private static String getQuboleBaseURL() {
    LOG.debug("Api Base is: " + quboleBaseURL);
    return quboleBaseURL;
  }

  public static String getInterpreterSyncLocation(){
    return confLoc;
  }

  public static void fetchFromObjectStore(List<String> noteIds) throws IOException {
    if (noteIds.isEmpty()) {
      LOG.info("No notes for this cluster");
      return;
    }
    File includesFile = createTempFileWithNoteIds(noteIds);
    File tempDownloadDir = createTempDownloadDirectory();

    if (!tempDownloadDir.exists()) {
      return;
    }
    String downloadCmd = "";
    if (!useHadoopCmd()) {
      String s3DownloadPath = S3Loc.endsWith("/") ? S3Loc : S3Loc + "/";
      downloadCmd = s3cmd + " get -r --skip-existing --rinclude-from="
              + includesFile.getAbsolutePath() + " --rexclude=.*  " + s3DownloadPath + " "
              + tempDownloadDir.getAbsolutePath();
    }
    else {
      String downloadPath = S3Loc.endsWith("/") ? S3Loc : S3Loc + "/";
      CommandManager mgr = CommandManager.getInstance();
      Command getCmd = mgr.createCommand(ObjectStoreOperationsEnum.GET);
      String source = "";
      for (String noteId : noteIds) {
        // '*' is used for regex
        source = source + " " + downloadPath + noteId + "*";
      }
      getCmd.setSource(source);
      getCmd.setDestination(tempDownloadDir.getAbsolutePath());
      downloadCmd = getCmd.getCommand();
      LOG.debug("downloadCmd: " + downloadCmd);
    }

    int numRetries = 4;
    for (int i = 0; i < numRetries; i++) {
      try {
        Process process = Runtime.getRuntime().exec(downloadCmd);
        String line;
        final BufferedReader inputStream = new BufferedReader(
            new InputStreamReader(process.getInputStream()));

        File file = new File("/usr/lib/zeppelin/logs/s3_init.log");

        // if file doesnt exists, then create it
        if (!file.exists()) {
          file.createNewFile();
        }

        FileWriter fw = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fw);

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        bw.write("===========" + dateFormat.format(date) + " Downloading notebooks ========");
        bw.newLine();
        while ((line = inputStream.readLine()) != null) {
          bw.write(line);
          bw.newLine();
        }
        bw.close();

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

  public static void fetchFromObjectStoreFolders(Map<String, Map<String, String>> notesAttributes)
      throws IOException {

    if (notesAttributes.isEmpty()) {
      LOG.info("No notes for this cluster");
      return;
    }

    List<Future<Integer>> futures = new ArrayList<>();
    for (String noteId : notesAttributes.keySet()) {
      Map<String, String> attrMap = notesAttributes.get(noteId);
      String s3loc = attrMap.get(QuboleNoteAttributes.LOCATION);
      if (s3loc != null) {
        Future<Integer> future = getNoteBook(noteId, s3loc);
        if (future != null){
          futures.add(future);
        }
      }
    }

    for (Future<Integer> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("ops Error occured when fetching notebooks", e);
      }
    }

    cleanNotesDownloadDirectory(new File(zepConfig.getNotebookDir()));
  }

  private static Future<Integer> getNoteBook(String noteId, String s3Loc) {
    Future<Integer> futureTask = null;
    Path notePath = Paths.get(zepConfig.getNotebookDir(), noteId, "note.json");
    File file = new File(notePath.toString());
    if (!file.exists()) {
      try {
        CommandManager mgr = CommandManager.getInstance();
        Command getCmd = mgr.createCommand(ObjectStoreOperationsEnum.GET);
        getCmd.setSource(s3Loc);
        getCmd.setDestination(notePath.toString());
        futureTask = mgr.executeCommand(getCmd);
      } catch (Exception e) {
        LOG.error("ops Error occured when trying to fetch notebook", e);
      }
    }
    return futureTask;
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
    String apiPath = opsApiPathV2;
    LOG.info("Making GET request to " + apiPath + " to update notebook");

    HttpURLConnection connection = sendRequestToQuboleRails(apiPath, null, "GET");
    int responseCode = connection.getResponseCode();
    if (responseCode == 200) {
      LOG.info("GET request to rails successful");
      InputStream inputStream = connection.getInputStream();
      BufferedReader bis = new BufferedReader(new InputStreamReader(inputStream));
      String readLine = bis.readLine();
      Map<String, Map<String, String> > notesAttributes =
          QuboleNoteAttributes.extractAttributes(readLine);

      List<String> noteIds = new ArrayList<>(notesAttributes.keySet());
      fetchFromObjectStoreFolders(notesAttributes);
      Notebook notebook = CronJob.notebook;
      notebook.loadAllNotes();
      if (notesAttributes != null) {
        for (String noteId : notesAttributes.keySet()) {
          QuboleNoteAttributes.setNoteAttributes(notebook.getNote(noteId),
              notesAttributes.get(noteId));
        }
      }
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

  public static boolean useHadoopCmd() {
    return useHadoopCmd;
  }

  public static Note downloadNoteIfNull(Notebook notebook, Note note, String noteId) {
    if (note == null) {
      // The note download might have failed for some reason
      // Try to download note from object store and add here.
      // this can be triggered only from FCN UI
      try {
        note = notebook.fetchAndLoadNoteFromObjectStore(noteId, null);
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

  public static void syncNotesToFolders() {
    CommandManager instance = CommandManager.getInstance();
    List<Note> allNotes = CronJob.notebook.getAllNotes();
    for (Note note : allNotes) {
      // noteAttributes will be null for Example notebooks
      QuboleNoteAttributes noteAttr = note.getQuboleNoteAttributes();
      if (noteAttr != null && noteAttr.getLocation() != null && !noteAttr.getLocation().isEmpty()) {
        Command syncCommand = instance.createCommand(ObjectStoreOperationsEnum.SYNC);
        URI uri = Paths.get(QuboleUtil.getNotebookDir(), note.id(), "note.json").toUri();
        syncCommand.setSource(uri.toString());
        syncCommand.setDestination(note.getQuboleNoteAttributes().getLocation());
        syncCommand.setNoteId(note.getId());
        instance.executeCommand(syncCommand);
      }
    }
  }

  public static void putInterpretersToObjectStore() {
    CommandManager instance = CommandManager.getInstance();
    Command putsCommand = instance.createCommand(ObjectStoreOperationsEnum.PUT);
    Path source = Paths.get(CronJob.notebook.getConf().getConfDir(), "interpreter.json");
    putsCommand.setSource(source.toString());
    putsCommand.setDestination(QuboleUtil.getInterpreterSyncLocation() + "/interpreter.json");
    instance.executeCommand(putsCommand);
  }

  public static void initNoteBookSync(ZeppelinConfiguration conf) {
    LOG.info("ops Using folder based sync in Zeppelin");

    int notebookSyncFrequency = conf.getNotebookSyncFrequency();
    int interpreterSyncFrequency = conf.getInterpreterSyncFrequency();
    LOG.info("ops Notebook sync frequency: " + notebookSyncFrequency
        + "\n interpreter frequency: " + interpreterSyncFrequency);

    LOG.info("ops Interpreter sync frequency: " + interpreterSyncFrequency);

    ScheduledExecutorService service = Executors.newScheduledThreadPool(5);
    service.scheduleWithFixedDelay(new Runnable() {

      @Override
      public void run() {
        try {
          QuboleUtil.syncNotesToFolders();
        } catch (Exception e) {
          LOG.error("ops", e);
        }
      }
    }, 0, notebookSyncFrequency, TimeUnit.MILLISECONDS);

    service.scheduleWithFixedDelay(new Runnable() {

      @Override
      public void run() {
        QuboleUtil.putInterpretersToObjectStore();
      }
    }, 0, interpreterSyncFrequency, TimeUnit.MILLISECONDS);
  }

  public static String getResponseFromConnection(HttpURLConnection conn) throws Exception {
    int responseCode = conn.getResponseCode();
    StringBuilder builder = new StringBuilder();
    InputStream inputStream;
    if (responseCode == 200) {
      inputStream = conn.getInputStream();
    } else {
      inputStream = conn.getErrorStream();
    }
    BufferedReader bis = new BufferedReader(new InputStreamReader(inputStream));
    String tmpStr;
    while ((tmpStr = bis.readLine()) != null) {
      builder.append(tmpStr);
    }
    String response = builder.toString();

    if (responseCode != 200) {
      throw new Exception(response);
    }
    return response;
  }
}
