package org.apache.zeppelin.interpreter.remote;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This thread sends paragraph's append-data
 * periodically, rather than continously, with
 * a period of BUFFER_TIME_MS. It handles append-data
 * for all paragraphs across all notebooks.
 */
public class AppendOutputRunner implements Runnable {

  private static final Logger logger =
      LoggerFactory.getLogger(AppendOutputRunner.class);
  private static RemoteInterpreterProcessListener listener;

  private static final Long BUFFER_TIME_MS = new Long(100);
  private static final Long SAFE_PROCESSING_TIME = new Long(10);
  private static final Long SAFE_PROCESSING_STRING_SIZE = new Long(100000);

  private static final BlockingQueue<AppendOutputBuffer> QUEUE =
      new LinkedBlockingQueue<AppendOutputBuffer>();

  @Override
  public void run() {

    while (true) {
      Map<String, Map<String, StringBuilder> > noteMap = 
          new HashMap<String, Map<String, StringBuilder> >();
      List<AppendOutputBuffer> list = new LinkedList<AppendOutputBuffer>();

      /* "drainTo" method does not wait for any element
       * to be present in the queue, and thus this loop would
       * continuosly run (with period of BUFFER_TIME_MS). "take()" method
       * waits for the queue to become non-empty and then removes
       * one element from it. Rest elements from queue (if present) are
       * removed using "drainTo" method. Thus we save on some un-necessary
       * cpu-cycles.
       */
      try {
        list.add(QUEUE.take());
      } catch (InterruptedException e) {
        logger.error("Wait for OutputBuffer queue interrupted: " + e.getMessage());
        break;
      }
      Long processingStartTime = System.currentTimeMillis();
      QUEUE.drainTo(list);

      for (AppendOutputBuffer buffer: list) {
        String noteId = buffer.getNoteId();
        String paragraphId = buffer.getParagraphId();

        Map<String, StringBuilder> paragraphMap = (noteMap.containsKey(noteId)) ?
            noteMap.get(noteId) : new HashMap<String, StringBuilder>();
        StringBuilder builder = paragraphMap.containsKey(paragraphId) ?
            paragraphMap.get(paragraphId) : new StringBuilder();

        builder.append(buffer.getData());
        paragraphMap.put(paragraphId, builder);
        noteMap.put(noteId, paragraphMap);
      }
      Long processingTime = System.currentTimeMillis() - processingStartTime;

      if (processingTime > SAFE_PROCESSING_TIME) {
        logger.warn("Processing time for buffered append-output is high: " +
            processingTime + " milliseconds.");
      } else {
        logger.debug("Processing time for append-output took "
            + processingTime + " milliseconds");
      }

      Long sizeProcessed = new Long(0);
      for (String noteId: noteMap.keySet()) {
        for (String paragraphId: noteMap.get(noteId).keySet()) {
          String data = noteMap.get(noteId).get(paragraphId).toString();
          sizeProcessed += data.length();
          listener.onOutputAppend(noteId, paragraphId, data);
        }
      }

      if (sizeProcessed > SAFE_PROCESSING_STRING_SIZE) {
        logger.warn("Processing size for buffered append-output is high: " +
            sizeProcessed + " characters.");
      } else {
        logger.debug("Processing size for append-output is " +
            sizeProcessed + " characters");
      }
      try {
        Thread.sleep(BUFFER_TIME_MS);
      } catch (InterruptedException e) {
        logger.error("Append output thread interrupted: " + e.getMessage());
        break;
      }
    }
  }

  public static void appendBuffer(String noteId, String paragraphId, String outputToAppend) {
    QUEUE.offer(new AppendOutputBuffer(noteId, paragraphId, outputToAppend));
  }

  public static void setListener(RemoteInterpreterProcessListener listener) {
    AppendOutputRunner.listener = listener;
  }
}
