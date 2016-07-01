package org.apache.zeppelin.spark;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * This class is used to send qlog of a spark sql command
 * back to zeppelin server. Fetching qlog from dataFrame
 * and send occurs in async thread, so that none of spark
 * operations are affected in any way.
 */
public class Qlog {

  private static final Logger LOG = LoggerFactory.getLogger(Qlog.class);

  //TO-DO Find/create a config to fetch master-ip and port number
  private static final String baseUrl = "http://localhost:8082/api/notebook/";
  private static final ExecutorService asyncZeppelinEventExecutorService =
      Executors.newFixedThreadPool(5);

  private static final Integer INITIAL_BACKOFF_MS = 64;
  private static final Integer MAX_RETRIES = 5;
  public static final String SEND_QLOG = "sendQlog";

  public static void sendQlogAsync(final Object df,
      final String noteId, final String paragraphId) {
    try {
      asyncZeppelinEventExecutorService.execute(new Runnable() {
        @Override
        public void run() {
          Qlog.sendQlog(df, noteId, paragraphId);
        }
      });
    } catch (RejectedExecutionException | NullPointerException e) {
      LOG.info("Async qlog sending to zeppelin server failed " + e.getMessage());
    }
  }

  private static void sendQlog(Object df, String noteId, String paragraphId) {
    List<Map<String, String>> qlogList = getQlogFromDataFrame(df);
    if (qlogList == null) {
      return;
    }
    String qlog = constructQlogString(qlogList);
    JsonObject params = new JsonObject();
    params.addProperty("qlog", qlog);
    params.addProperty("noteId", noteId);
    params.addProperty("paragraphId", paragraphId);
    HttpURLConnection connection;
    int numRetry = 0;
    long backoff = INITIAL_BACKOFF_MS;

    while (numRetry < MAX_RETRIES) { // Exponential backoff retries
      try {
        connection = (HttpURLConnection) (new URL(baseUrl + "qlog"))
            .openConnection();
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        String jsonData = params.toString();
        OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
        out.write(jsonData);
        out.flush();
        out.close();
        int responseCode = connection.getResponseCode();
        if (responseCode == 200) {
          LOG.info("Qlog request to zeppelin server for paragraphId = " + paragraphId +
              "; and qlog =" + qlog + " succeeded");
          return;
        } else {
          LOG.info(responseCode + " error in sending qlog to Zeppelin."
              + " Will retry with exponential backoff");
          Thread.sleep(backoff);
          backoff = backoff * 2;
          numRetry += 1;
        }
      } catch (IOException | InterruptedException e) {
        LOG.error("Error while sending qlog to Zeppelin for "
            + "paragraphId = " + paragraphId + " of note = " + noteId, e);
        return;
      }
    }
    return;
  }

  public static void checkAndSendQlog(InterpreterContext interpreterContext, Object df) {
    Boolean isJobServerQuery = isJobServerQuery(interpreterContext);
    if (isJobServerQuery) {
      sendQlogAsync(df, interpreterContext.getNoteId(),
        interpreterContext.getParagraphId());
    }
  }

  public static Boolean isJobServerQuery(InterpreterContext interpreterContext) {
    Map<String, Object> contextConfig = interpreterContext.getConfig();
    return (contextConfig.containsKey(SEND_QLOG)
        && contextConfig.get(SEND_QLOG).equals(true));
  }

  private static List<Map<String, String>> getQlogFromDataFrame(Object df) {
    List<Map<String, String>> qlogList = new ArrayList<>();
    try {
      StructType structType = (StructType) df.getClass().getMethod("schema").invoke(df);
      for (StructField structField: structType.fields()) {
        Map<String, String> map = new HashMap<String, String>();
        map.put("ColumnType", structField.dataType().simpleString());
        map.put("ColumnName", structField.name());
        qlogList.add(map);
      }
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
        | NoSuchMethodException | SecurityException e) {
      LOG.error("Exception while getting qlog schema " + e);
    }
    return qlogList;
  }

  private static String constructQlogString(List<Map<String, String>> qlogList) {
    Map<String, Map<String, List>> m2 = new HashMap<>();
    Map<String, List> m1 = new HashMap<>();
    m1.put("query0", qlogList);
    m2.put("QBOL-QUERY-SCHEMA", m1);
    return (new Gson()).toJson(m2);
  }
}
