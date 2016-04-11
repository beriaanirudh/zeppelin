package org.apache.zeppelin.events;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zeppelin.events.QuboleEventsEnum.EVENTTYPE;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.util.QuboleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Utility to deal with zeppelin events
 */
public class QuboleEventUtils {

  private static final Logger LOG = LoggerFactory.getLogger(QuboleEventUtils.class);
  private static ExecutorService executorService;

  /**
   * This method needs to be called after actual execution of the operation so
   * that any exception/error does not hinder the actual opertion
   *
   */
  public static void saveEvent(final QuboleEventsEnum.EVENTTYPE eventType, String qbolUserId,
      HashMap<Object, Object> infos, Object obj) {
    try {
      final QuboleEvent event = new QuboleEvent(eventType, qbolUserId);
      event.setObject(obj);
      event.setInfos(infos);
      // applies only for paragraph execution start and end
      final QuboleParaEventIdObject paraEventIdObject = getParaEventIdObject(eventType, obj);
      Runnable task = new Runnable() {
        @Override
        public void run() {
          if (EVENTTYPE.PARAGRAPH_EXECUTION_END == eventType) {
            if (!waitForParagraphEvent()) {
              LOG.error("Event id is null. Not sending request to rails. Event:" + getJSON(event));
              return;
            }
          }
          HttpURLConnection connection = QuboleUtil.sendEvent(getJSON(event));
          // Remember the event_id, to send it back in PARAGRAPH_EXECUTION_END
          if (EVENTTYPE.PARAGRAPH_EXECUTION_START == eventType) {
            setEventId(connection);
          }
        }

        private String getJSON(QuboleEvent event) {
          if (event == null) {
            return null;
          }
          GsonBuilder gsonBuilder = new GsonBuilder();
          gsonBuilder.setPrettyPrinting();
          Gson gson = gsonBuilder.create();
          String json = gson.toJson(event);
          return json;
        }

        private String getEventId(HttpURLConnection conn) {
          String eventId = null;
          try {
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuffer strBuf = new StringBuffer();
            String inputLine;
            while ((inputLine = in.readLine()) != null)
              strBuf.append(inputLine);
            in.close();
            JsonObject propObj = (JsonObject) new JsonParser().parse(strBuf.toString());
            JsonElement jsonElement = propObj.get("event_id");
            eventId = jsonElement.getAsString();
          } catch (IOException e) {
            LOG.error("Execption when parsing response from rails", e);
          }
          return eventId;
        }

        private void setEventId(HttpURLConnection connection) {
          if (connection != null && paraEventIdObject != null)
            paraEventIdObject.setParaEventId(getEventId(connection));
        }

        /**
         * @return true if eventid is received, false otherwise
         */
        private boolean waitForParagraphEvent() {
          if (paraEventIdObject == null)
            return false;
          if (paraEventIdObject.getParaEventId() == null) {
            try {
              // event id propagation to zeppelin from rails may be delayed
              // due to network latency etc. Adding a 5 sec wait
              // to mitigate the above.
              Thread.sleep(5000);
            } catch (InterruptedException e) {
              LOG.error("Interupted when waiting for event id", e);
            }
          }
          String parEventId = paraEventIdObject.getParaEventId();
          // Dont send execution end event when event id is null
          if (parEventId == null) {
            return false;
          }
          event.setEvent_id(paraEventIdObject.getParaEventId());
          return true;
        }
      };
      createOrGetExecutorService().execute(task);
    } catch (Exception e) {
      LOG.error("Error when saving event  ", e);
    }
  }

  private static QuboleParaEventIdObject getParaEventIdObject(QuboleEventsEnum.EVENTTYPE eventType,
      Object obj) {
    QuboleParaEventIdObject idObj = null;
    if (obj instanceof Paragraph) {
      if (eventType == EVENTTYPE.PARAGRAPH_EXECUTION_START) {
        idObj = new QuboleParaEventIdObject();
        ((Paragraph) obj).setQbolEventId(idObj);
      }
      if (eventType == EVENTTYPE.PARAGRAPH_EXECUTION_END) {
        idObj = ((Paragraph) obj).getQbolEventId();
      }
    }
    return idObj;
  }

  public static void saveEvent(final QuboleEventsEnum.EVENTTYPE event, String qbolUserId,
      Object objs) {
    saveEvent(event, qbolUserId, null, objs);
  }

  public static void saveEvent(final QuboleEventsEnum.EVENTTYPE event, String qbolUserId) {
    saveEvent(event, qbolUserId, null, null);
  }

  private static ExecutorService createOrGetExecutorService() {
    if (executorService == null) {
      executorService = Executors.newFixedThreadPool(8);
    }
    return executorService;
  }
}
