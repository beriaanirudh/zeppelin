package org.apache.zeppelin.util;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterFactory;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.interpreter.LazyOpenInterpreter;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService.Client;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.NoteInterpreterLoader;
import org.apache.zeppelin.notebook.Notebook.CronJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class starts persistents interpreters pro-actively
 * and runs their boot-strap note-books.
 */
public class PersistentIntpsAndBootstrapNotes implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(PersistentIntpsAndBootstrapNotes.class);
  private static final String PERSISTENT_PROPERTY = "zeppelin.interpreter.persistent";
  private static final String PERSISTENT_NOTEBOOK = "zeppelin.interpreter.bootstrap.notebook";
  public static final String SPARK_CLASSNAME = "org.apache.zeppelin.spark.SparkInterpreter";

  private static final ScheduledExecutorService service =
      Executors.newSingleThreadScheduledExecutor();

  private PersistentIntpsAndBootstrapNotes()
  {}

  @Override
  public void run() {
    /* Any exception in run() will result in
     * stopping the scheduler, so we should Catch'em All.
     */
    try {
      Notebook notebook = CronJob.notebook;
      InterpreterFactory intpFactory = notebook.getInterpreterFactory();
      List<InterpreterSetting> settings = intpFactory.get();
      for (InterpreterSetting setting: settings) {
        checkPersistence(setting, intpFactory, notebook.getAllNotes());
      }
    } catch (Exception e) {
      LOG.error("Error while trying to start persistent interpreters", e);
    }
  }

  public static void schedulePeristentInterpreters() {
    PersistentIntpsAndBootstrapNotes persistentIntp = new PersistentIntpsAndBootstrapNotes();
    LOG.info("Starting a scheduler to start persistent interpreters and "
        + "run bootstrap notebooks");
    ZeppelinConfiguration conf = ZeppelinConfiguration.create();
    service.scheduleWithFixedDelay(persistentIntp, 0,
        conf.getPersistentInterpreterCheckDelay(), TimeUnit.MILLISECONDS);
  }


  public static void checkPersistence(InterpreterSetting setting,
      InterpreterFactory intpFactory, List<Note> notes) {
    String persistent = setting.getProperties().getProperty(PERSISTENT_PROPERTY);
    if (persistent != null && "true".equalsIgnoreCase(persistent)) {
      /* Exception in one interpreter/bootstrap notebook should not 
       * stop other interpreters.
       */
      try {
        InterpreterGroup intpGroup = setting.getInterpreterGroup(NoteInterpreterLoader.SHARED_SESSION);
        String email = intpGroup.getProperty(QuboleUtil.SPARK_YARN_QUEUE);
        //find userId from email, else qubole events wont have email
        String userId = QuboleUtil.getUserForEmail(email);
        runBootStrapAndStartIntp(setting, userId, email, intpFactory, notes);
      } catch (Exception e) {
        LOG.error("Exception while trying to start persistent interpreter "
            + setting.getName() + " with id = " + setting.id(), e);
      }
    }
  }
  
  private static boolean runBootStrapNotebook(InterpreterSetting setting, String userId,
      String email, List<Note> notes) {
    String bootstrapNoteId = setting.getProperties().getProperty(PERSISTENT_NOTEBOOK);
    Integer noteId;
    try {
      noteId = Integer.parseInt(bootstrapNoteId);
      if (noteId <= 0) return false;
    } catch (NumberFormatException e) {
      return false;
    }
    boolean ranParaForInterpreter = false;
    Note bootstrapNote = null;
    for (Note note: notes) {
      QuboleNoteAttributes attrs = note.getQuboleNoteAttributes();
      if (attrs == null) continue;
      if (attrs.getNoteId() != null && attrs.getNoteId().equals(noteId)) {
        bootstrapNote = note;
        break;
      }
    }
    if (bootstrapNote != null) {
      LOG.info("Running bootstrap note = " + noteId + " for intepreter "
          + setting.getName() + " with id = " + setting.id());
      ranParaForInterpreter = bootstrapNote.runParagraphsForBootstrap(setting, userId, email);
    }
    return ranParaForInterpreter;
  }

  public static void runBootStrapAndStartIntp(InterpreterSetting setting, String userId,
      String email, InterpreterFactory intpFactory, List<Note> notes) {
    if (!proceedWithBootStrap(setting)) {
      return;
    }
    if (!isInterpreterUp(setting)) {
      boolean ranParaForIntp = false;
      try {
        ranParaForIntp = runBootStrapNotebook(setting, userId, email, notes);
      } catch (Exception e) {
        LOG.error("Error while attempting to run bootstrap notebook for interpreter " +
            setting.getName() + " with id=" + setting.id(), e);
      }
      /* If this is a persistent interpreter without
       * any note bootstrap, or the bootstrap does not
       * have any paragraph to be run with this interpreter,
       * we need to start it pro-actively.
       */
      if (!ranParaForIntp) {
        intpFactory.restart(setting.id());
        intpFactory.createInterpretersForNote(setting, NoteInterpreterLoader.SHARED_SESSION, NoteInterpreterLoader.SHARED_SESSION);
        InterpreterGroup group = setting.getInterpreterGroup(null);
        LOG.info("Starting persistent interpreter = " + setting.getName() + " with id = " + setting.id()
                 + "since either there was no bootstrap notebook or"
                 + "bootstrap did not use this interpreter.");
        for (List<Interpreter> intpCollect: group.values()) {
          for (Interpreter intp: intpCollect) {
            /* Following from ZEP-551, it was observed that due
             * to a deadlock bug in OSS R interpreter, 'opening' R
             * interpreter stalled the Remote JVM completely. For our use-cases of
             * persistent interpreters, 'opening' the Spark interpreter suffices
             * since it starts the remote JVM. Also, by not pro-actively opening
             * other interpreters, we save our-selves from any other
             * rogue interpreter.
             */
            if (SPARK_CLASSNAME.equals(intp.getClassName())) {
              PersistentInterpreterStarter.startPersistentInterpreter(intp);
            }
          }
        }
      }
    }
  }

  private static boolean proceedWithBootStrap(InterpreterSetting setting) {
    // Only spark interpreters can be persistent or have bootstrap notebooks
    if (QuboleUtil.sparkInterpreterGroupName.equals(setting.getGroup())) {
      return true;
    }
    return false;
  }

  private static boolean isInterpreterUp(InterpreterSetting setting) {
    InterpreterGroup group = setting.getInterpreterGroup(NoteInterpreterLoader.SHARED_SESSION);
    RemoteInterpreterProcess intpProcess = group.getRemoteInterpreterProcess();

    //Check whether Remote interpreter is running
    if (intpProcess == null || !intpProcess.isRunning()) {
      return false;
    }
    Client client = null;
    try {
      client = group.getRemoteInterpreterProcess().getClient();
    } catch (Exception e) {
      LOG.info("Error whlile trying to get client for remote interpreter. Will restart", e);
      return false;
    } finally {
      try {
        if (client != null) {
          group.getRemoteInterpreterProcess().releaseClient(client);
        }
      } catch (Exception e) {
        LOG.debug("Exception while trying to release client", e);
      }
    }
    return true;
  }

  public static String getPeristentPropertyForUnitTest() {
    return PERSISTENT_PROPERTY;
  }

  public static String getBootstrapNotebookForUnitTest() {
    return PERSISTENT_NOTEBOOK;
  }
}
