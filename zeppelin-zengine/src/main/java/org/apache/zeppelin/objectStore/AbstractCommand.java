package org.apache.zeppelin.objectStore;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook.CronJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Abstract class for all objectstore operations
 *
 */

public abstract class AbstractCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractCommand.class);
  private String source;
  private String destination;
  private String noteId;

  @Override
  public String getSource() {
    return source;
  }

  @Override
  public void setSource(String source) {
    this.source = source;
  }

  @Override
  public String getDestination() {
    return destination;
  }

  @Override
  public void setDestination(String dest) {
    this.destination = dest;

  }
  
  @Override
  public String getNoteId() {
    return noteId;
  }

  @Override
  public void setNoteId(String noteId) {
    this.noteId = noteId;
  }

  @Override
  public boolean validate() {
    String noteId = getNoteId();
    Note note = null;
    if (!StringUtils.isEmpty(noteId)) {
      note = CronJob.notebook.getNote(noteId);
    }
    return (!StringUtils.isEmpty(getSource()) && !StringUtils.isEmpty(getDestination()))
        && (note == null || note.getQuboleNoteAttributes() == null
        || note.getQuboleNoteAttributes().getLocation().equals(getDestination()));
  }

  @Override
  public int execute() throws IOException, InterruptedException {
    return s3Exec();
  }

  public int s3Exec() throws IOException, InterruptedException {
    if (!validate()) {
      throw (new RuntimeException("Validation failed"));
    }
    int numRetries = 2;
    int exitValue = -1;
    while (numRetries > 0) {
      String command = getCommand();
      Process process = Runtime.getRuntime().exec(command);
      process.waitFor();
      exitValue = process.exitValue();
      if (exitValue == 0 && isSuccess()) {
        LOG.debug("ops " + getOperation() + " Operation succeeded. Command: " + command);
        break;
      }
      LOG.error("ops operation failed for command: " + command);
      Thread.sleep(5000);
      numRetries--;
    }
    return exitValue;
  }

  @Override
  public boolean isSuccess() {
    return true;
  }
}
