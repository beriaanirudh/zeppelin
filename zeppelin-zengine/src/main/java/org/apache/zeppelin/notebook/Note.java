/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.notebook;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.Input;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.remote.RemoteAngularObjectRegistry;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.events.QuboleEventUtils;
import org.apache.zeppelin.events.QuboleEventsEnum.EVENTTYPE;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.notebook.Notebook.CronJob;
import org.apache.zeppelin.notebook.repo.NotebookRepo;
import org.apache.zeppelin.notebook.utility.IdHashes;
import org.apache.zeppelin.resource.ResourcePoolUtils;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.scheduler.Job.Status;
import org.apache.zeppelin.scheduler.JobListener;
import org.apache.zeppelin.search.SearchService;
import org.apache.zeppelin.util.PersistentIntpsAndBootstrapNotes;

import com.google.gson.Gson;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.user.Credentials;
import org.apache.zeppelin.util.QuboleNoteAttributes;
import org.apache.zeppelin.util.QuboleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binded interpreters for a note
 */
public class Note implements Serializable, JobListener {
  static Logger logger = LoggerFactory.getLogger(Note.class);
  private static final long serialVersionUID = 7920699076577612429L;
  private static final String DIFF_INTP_BOOTSTRAP_MSG = "Paragraph not run as a part " +
      "of bootstrap since it is bound to a diff spark interpreter";

  // threadpool for delayed persist of note
  private static final ScheduledThreadPoolExecutor delayedPersistThreadPool =
          new ScheduledThreadPoolExecutor(0);
  static {
    delayedPersistThreadPool.setRemoveOnCancelPolicy(true);
  }

  final List<Paragraph> paragraphs = new LinkedList<>();

  private String name = "";
  private String id;

  private transient ZeppelinConfiguration conf = ZeppelinConfiguration.create();

  @SuppressWarnings("rawtypes")
  Map<String, List<AngularObject>> angularObjects = new HashMap<>();

  private transient NoteInterpreterLoader replLoader;
  private transient JobListenerFactory jobListenerFactory;
  private transient NotebookRepo repo;
  private transient SearchService index;
  private transient ScheduledFuture delayedPersist;
  private transient QuboleNoteAttributes quboleNoteAttributes;

  public QuboleNoteAttributes getQuboleNoteAttributes() {
    return quboleNoteAttributes;
  }

  public void setQuboleNoteAttributes(QuboleNoteAttributes quboleNoteAttributes) {
    this.quboleNoteAttributes = quboleNoteAttributes;
  }

  private transient Credentials credentials;

  /**
   * note configurations.
   *
   * - looknfeel - cron
   */
  private Map<String, Object> config = new HashMap<>();

  /**
   * note information.
   *
   * - cron : cron expression validity.
   */
  private Map<String, Object> info = new HashMap<>();

  private String source;

  public Note() {}

  public Note(NotebookRepo repo, NoteInterpreterLoader replLoader,
      JobListenerFactory jlFactory, SearchService noteIndex, Credentials credentials) {
    this.repo = repo;
    this.replLoader = replLoader;
    this.jobListenerFactory = jlFactory;
    this.index = noteIndex;
    this.credentials = credentials;
    generateId();
  }

  private void generateId() {
    id = IdHashes.encode(System.currentTimeMillis() + new Random().nextInt());
  }

  public String id() {
    return id;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  private String normalizeNoteName(String name){
    name = name.trim();
    name = name.replace("\\", "/");
    while (name.indexOf("///") >= 0) {
      name = name.replaceAll("///", "/");
    }
    name = name.replaceAll("//", "/");
    if (name.length() == 0) {
      name = "/";
    }
    return name;
  }

  public void setName(String name) {
    if (name.indexOf('/') >= 0 || name.indexOf('\\') >= 0) {
      name = normalizeNoteName(name);
    }
    this.name = name;
  }

  public NoteInterpreterLoader getNoteReplLoader() {
    return replLoader;
  }

  public void setReplLoader(NoteInterpreterLoader replLoader) {
    this.replLoader = replLoader;
  }

  public JobListenerFactory getJobListenerFactory() {
    return jobListenerFactory;
  }

  public void setJobListenerFactory(JobListenerFactory jobListenerFactory) {
    this.jobListenerFactory = jobListenerFactory;
  }

  public NotebookRepo getNotebookRepo() {
    return repo;
  }

  public void setNotebookRepo(NotebookRepo repo) {
    this.repo = repo;
  }

  public void setIndex(SearchService index) {
    this.index = index;
  }

  public Credentials getCredentials() {
    return credentials;
  };

  public void setCredentials(Credentials credentials) {
    this.credentials = credentials;
  }


  @SuppressWarnings("rawtypes")
  public Map<String, List<AngularObject>> getAngularObjects() {
    return angularObjects;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  /**
   * Add paragraph last.
   */

  public Paragraph addParagraph() {
    Paragraph p = new Paragraph(this, this, replLoader);
    synchronized (paragraphs) {
      paragraphs.add(p);
    }
    return p;
  }

  /**
   * Clone paragraph and add it to note.
   *
   * @param srcParagraph
   */
  public void addCloneParagraph(Paragraph srcParagraph) {

    // Keep paragraph original ID
    final Paragraph newParagraph = new Paragraph(srcParagraph.getId(), this, this, replLoader);

    Map<String, Object> config = new HashMap<>(srcParagraph.getConfig());
    Map<String, Object> param = new HashMap<>(srcParagraph.settings.getParams());
    Map<String, Input> form = new HashMap<>(srcParagraph.settings.getForms());

    newParagraph.setConfig(config);
    newParagraph.settings.setParams(param);
    newParagraph.settings.setForms(form);
    newParagraph.setText(srcParagraph.getText());
    newParagraph.setTitle(srcParagraph.getTitle());

    try {
      Gson gson = new Gson();
      String resultJson = gson.toJson(srcParagraph.getReturn());
      InterpreterResult result = gson.fromJson(resultJson, InterpreterResult.class);
      newParagraph.setReturn(result, null);
    } catch (Exception e) {
      // 'result' part of Note consists of exception, instead of actual interpreter results
      logger.warn("Paragraph " + srcParagraph.getId() + " has a result with exception. "
              + e.getMessage());
    }

    synchronized (paragraphs) {
      paragraphs.add(newParagraph);
    }
  }

  /**
   * Insert paragraph in given index.
   *
   * @param index
   */
  public Paragraph insertParagraph(int index) {
    Paragraph p = new Paragraph(this, this, replLoader);
    synchronized (paragraphs) {
      paragraphs.add(index, p);
    }
    return p;
  }

  /**
   * Remove paragraph by id.
   *
   * @param paragraphId
   * @return a paragraph that was deleted, or <code>null</code> otherwise
   */
  public Paragraph removeParagraph(String paragraphId) {
    if (replLoader != null) {
      removeAllAngularObjectInParagraph(paragraphId);
    }
    ResourcePoolUtils.removeResourcesBelongsToParagraph(id(), paragraphId);
    synchronized (paragraphs) {
      Iterator<Paragraph> i = paragraphs.iterator();
      while (i.hasNext()) {
        Paragraph p = i.next();
        if (p.getId().equals(paragraphId)) {
          if (index != null){
            index.deleteIndexDoc(this, p);
          }
          i.remove();
          return p;
        }
      }
    }


    return null;
  }

  /**
   * Clear paragraph output by id.
   *
   * @param paragraphId
   * @return
   */
  public Paragraph clearParagraphOutput(String paragraphId) {
    synchronized (paragraphs) {
      for (int i = 0; i < paragraphs.size(); i++) {
        Paragraph p = paragraphs.get(i);
        if (p.getId().equals(paragraphId)) {
          p.setReturn(null, null);
          p.clearRuntimeInfo(null);
          return p;
        }
      }
    }
    return null;
  }

  /**
   * Move paragraph into the new index (order from 0 ~ n-1).
   *
   * @param paragraphId
   * @param index new index
   */
  public void moveParagraph(String paragraphId, int index) {
    moveParagraph(paragraphId, index, false);
  }

  /**
   * Move paragraph into the new index (order from 0 ~ n-1).
   *
   * @param paragraphId
   * @param index new index
   * @param throwWhenIndexIsOutOfBound whether throw IndexOutOfBoundException
   *                                   when index is out of bound
   */
  public void moveParagraph(String paragraphId, int index, boolean throwWhenIndexIsOutOfBound) {
    synchronized (paragraphs) {
      int oldIndex;
      Paragraph p = null;

      if (index < 0 || index >= paragraphs.size()) {
        if (throwWhenIndexIsOutOfBound) {
          throw new IndexOutOfBoundsException("paragraph size is " + paragraphs.size() +
              " , index is " + index);
        } else {
          return;
        }
      }

      for (int i = 0; i < paragraphs.size(); i++) {
        if (paragraphs.get(i).getId().equals(paragraphId)) {
          oldIndex = i;
          if (oldIndex == index) {
            return;
          }
          p = paragraphs.remove(i);
        }
      }

      if (p != null) {
        paragraphs.add(index, p);
      }
    }
  }

  public boolean isLastParagraph(String paragraphId) {
    if (!paragraphs.isEmpty()) {
      synchronized (paragraphs) {
        if (paragraphId.equals(paragraphs.get(paragraphs.size() - 1).getId())) {
          return true;
        }
      }
      return false;
    }
    /** because empty list, cannot remove nothing right? */
    return true;
  }

  public Paragraph getParagraph(String paragraphId) {
    synchronized (paragraphs) {
      for (Paragraph p : paragraphs) {
        if (p.getId().equals(paragraphId)) {
          return p;
        }
      }
    }
    return null;
  }

  public Paragraph getLastParagraph() {
    synchronized (paragraphs) {
      return paragraphs.get(paragraphs.size() - 1);
    }
  }

  public List<Map<String, String>> generateParagraphsInfo (){
    List<Map<String, String>> paragraphsInfo = new LinkedList<>();
    synchronized (paragraphs) {
      for (Paragraph p : paragraphs) {
        Map<String, String> info = new HashMap<>();
        info.put("id", p.getId());
        info.put("status", p.getStatus().toString());
        if (p.getDateStarted() != null) {
          info.put("started", p.getDateStarted().toString());
        }
        if (p.getDateFinished() != null) {
          info.put("finished", p.getDateFinished().toString());
        }
        if (p.getStatus().isRunning()) {
          info.put("progress", String.valueOf(p.progress()));
        }
        paragraphsInfo.add(info);
      }
    }
    return paragraphsInfo;
  }

  /**
   * Run all paragraphs sequentially.
   */
  public void runAll() {
    String cronExecutingUser = (String) getConfig().get("cronExecutingUser");
    List<Paragraph> newParaList = null;
    synchronized (paragraphs) {
      newParaList = new LinkedList<Paragraph>(paragraphs);
    }
    for (Paragraph p : newParaList) {
      if (!p.isEnabled()) {
        continue;
      }
      AuthenticationInfo authenticationInfo = new AuthenticationInfo();
      authenticationInfo.setUser(cronExecutingUser);
      p.setAuthenticationInfo(authenticationInfo);
      p.setNoteReplLoader(replLoader);
      run(p.getId(), cronExecutingUser, QuboleUtil.getEmailForUser(cronExecutingUser), false);
      QuboleEventUtils.saveEvent(EVENTTYPE.PARAGRAPH_EXECUTION_START, cronExecutingUser, p);
    }
  }

  /**
   * Run a single paragraph.
   *
   * @param paragraphId
   */
  public void run(String paragraphId, String userId, String email, boolean bootStrapRun) {
    Paragraph p = getParagraph(paragraphId);
    p.setNoteReplLoader(replLoader);
    p.setListener(jobListenerFactory.getParagraphJobListener(this));
    p.clearRuntimeInfo(null);

    if (!bootStrapRun) {
      InterpreterSetting setting = getSettingFromInterpreter(
          replLoader.get(p.getRequiredReplName()));
      InterpreterFactory intpFactory = CronJob.notebook.getInterpreterFactory();
      PersistentIntpsAndBootstrapNotes.runBootStrapAndStartIntp(setting, userId, email,
          intpFactory, CronJob.notebook.getAllNotes());
    }
    String requiredReplName = p.getRequiredReplName();
    Interpreter intp = replLoader.get(requiredReplName);
    if (intp == null) {
      // TODO(jongyoul): Make "%jdbc" configurable from JdbcInterpreter
      if (conf.getUseJdbcAlias() && null != (intp = replLoader.get("jdbc"))) {
        String pText = p.getText().replaceFirst(requiredReplName, "jdbc(" + requiredReplName + ")");
        logger.debug("New paragraph: {}", pText);
        p.setEffectiveText(pText);
      } else {
        String intpExceptionMsg = String.format("%s",
          p.getJobName()
          + "'s Interpreter "
          + requiredReplName + " not found"
        );
        InterpreterException intpException = new InterpreterException(intpExceptionMsg);
        InterpreterResult intpResult = new InterpreterResult(
          InterpreterResult.Code.ERROR, intpException.getMessage()
        );
        p.setReturn(intpResult, intpException);
        p.setStatus(Job.Status.ERROR);
        throw intpException;
      }
    }
    if (p.getConfig().get("enabled") == null || (Boolean) p.getConfig().get("enabled")) {
      intp.getScheduler().submit(p);
    }
  }

  public void run(String paragraphId, String userId, String email) {
    run(paragraphId, userId, email, false);
  }

  /**
   * Check whether all paragraphs belongs to this note has terminated
   * @return
   */
  public boolean isTerminated() {
    synchronized (paragraphs) {
      for (Paragraph p : paragraphs) {
        if (!p.isTerminated()) {
          return false;
        }
      }
    }

    return true;
  }

  public void run(String paragraphId) {
    logger.warn("Running para without user id");
    run(paragraphId, null, null);
  }

  public boolean runParagraphsForBootstrap(InterpreterSetting setting,
      String userId, String email) {
    boolean ranParaForInterpreter = false;
    List<Paragraph> newParaList = null;
    synchronized (paragraphs) {
      newParaList = new LinkedList<Paragraph>(paragraphs);
    }
    for (Paragraph p: newParaList) {
      InterpreterSetting paraIntpSetting = getSettingFromInterpreter(
          replLoader.get(p.getRequiredReplName()));
      if (QuboleUtil.sparkInterpreterGroupName.equals(
          paraIntpSetting.getGroup())) {
        if (paraIntpSetting.equals(setting)) {
          ranParaForInterpreter = true;
          run(p.getId(), userId, email, true);
        }
        //do not run any other spark interpreter
        else {
          p.setErrorResultForBootstrap(new InterpreterResult(
              Code.ERROR, Type.TEXT, DIFF_INTP_BOOTSTRAP_MSG));
        }
      } else {
        run(p.getId(), userId, email, true);
      }
    }
    return ranParaForInterpreter;
  }

  private InterpreterSetting getSettingFromInterpreter(Interpreter interpreter) {
    InterpreterFactory intpFactory = CronJob.notebook.getInterpreterFactory();
    for (InterpreterSetting setting: intpFactory.get()) {
      for (InterpreterGroup group: setting.getAllInterpreterGroups()) {
        for (List<Interpreter> intps: group.values()) {
          for (Interpreter intp: intps) {
            if (interpreter.equals(intp)) {
              return setting;
            }
          }
        }
      }
    }
    return null;
  }

  public List<InterpreterCompletion> completion(String paragraphId, String buffer, int cursor) {
    Paragraph p = getParagraph(paragraphId);
    p.setNoteReplLoader(replLoader);
    p.setListener(jobListenerFactory.getParagraphJobListener(this));
    List completion = p.completion(buffer, cursor);

    return completion;
  }

  public List<Paragraph> getParagraphs() {
    synchronized (paragraphs) {
      return new LinkedList<Paragraph>(paragraphs);
    }
  }

  private void snapshotAngularObjectRegistry() {
    angularObjects = new HashMap<>();

    List<InterpreterSetting> settings = replLoader.getInterpreterSettings();
    if (settings == null || settings.size() == 0) {
      return;
    }

    for (InterpreterSetting setting : settings) {
      InterpreterGroup intpGroup = setting.getInterpreterGroup(id);
      AngularObjectRegistry registry = intpGroup.getAngularObjectRegistry();
      angularObjects.put(intpGroup.getId(), registry.getAllWithGlobal(id));
    }
  }

  private void removeAllAngularObjectInParagraph(String paragraphId) {
    angularObjects = new HashMap<String, List<AngularObject>>();

    List<InterpreterSetting> settings = replLoader.getInterpreterSettings();
    if (settings == null || settings.size() == 0) {
      return;
    }

    for (InterpreterSetting setting : settings) {
      InterpreterGroup intpGroup = setting.getInterpreterGroup(id);
      AngularObjectRegistry registry = intpGroup.getAngularObjectRegistry();

      if (registry instanceof RemoteAngularObjectRegistry) {
        // remove paragraph scope object
        ((RemoteAngularObjectRegistry) registry).removeAllAndNotifyRemoteProcess(id, paragraphId);
      } else {
        registry.removeAll(id, paragraphId);
      }
    }
  }

  public void persist(AuthenticationInfo subject) throws IOException {
    stopDelayedPersistTimer();
    snapshotAngularObjectRegistry();
    index.updateIndexDoc(this);
    repo.save(this, subject);
  }

  /**
   * Persist this note with maximum delay.
   * @param maxDelaySec
   */
  public void persist(int maxDelaySec, AuthenticationInfo subject) {
    startDelayedPersistTimer(maxDelaySec, subject);
  }

  public void unpersist(AuthenticationInfo subject) throws IOException {
    repo.remove(id(), subject);
  }


  private void startDelayedPersistTimer(int maxDelaySec, final AuthenticationInfo subject) {
    synchronized (this) {
      if (delayedPersist != null) {
        return;
      }

      delayedPersist = delayedPersistThreadPool.schedule(new Runnable() {

        @Override
        public void run() {
          try {
            persist(subject);
          } catch (IOException e) {
            logger.error(e.getMessage(), e);
          }
        }
      }, maxDelaySec, TimeUnit.SECONDS);
    }
  }

  private void stopDelayedPersistTimer() {
    synchronized (this) {
      if (delayedPersist == null) {
        return;
      }

      delayedPersist.cancel(false);
    }
  }

  public Map<String, Object> getConfig() {
    if (config == null) {
      config = new HashMap<>();
    }
    return config;
  }

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }

  public Map<String, Object> getInfo() {
    if (info == null) {
      info = new HashMap<>();
    }
    return info;
  }

  public void setInfo(Map<String, Object> info) {
    this.info = info;
  }

  @Override
  public void beforeStatusChange(Job job, Status before, Status after) {
  }

  @Override
  public void afterStatusChange(Job job, Status before, Status after) {
  }

  @Override
  public void onProgressUpdate(Job job, int progress) {}
}
