package org.apache.zeppelin.util;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterFactory;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterOption;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.interpreter.mock.MockInterpreter1;
import org.apache.zeppelin.interpreter.mock.MockInterpreter11;
import org.apache.zeppelin.interpreter.mock.MockInterpreter2;
import org.apache.zeppelin.notebook.Note;
import org.junit.Before;
import org.junit.Test;

public class TestPersistentIntpsAndBootstrapNotebooks {

  @Before
  public void beforeTest() {
    Interpreter.registeredInterpreters = Collections
        .synchronizedMap(new HashMap<String, Interpreter.RegisteredInterpreter>());
    MockInterpreter1.register("mock1", "group1", "org.apache.zeppelin.interpreter.mock.MockInterpreter1");
    MockInterpreter11.register("mock11", "group1", "org.apache.zeppelin.interpreter.mock.MockInterpreter11");
    MockInterpreter2.register("mock2", "group2", "org.apache.zeppelin.interpreter.mock.MockInterpreter2");
  }

  @Test
  public void persistentInterprterWithoutBootstrap() {
    Properties properties = new Properties();
    properties.put(PersistentIntpsAndBootstrapNotes.getPeristentPropertyForUnitTest(), "true");
    InterpreterSetting setting = getSettingWithProperties(properties);
    InterpreterFactory intpFactory = mock(InterpreterFactory.class);
    PersistentIntpsAndBootstrapNotes.checkPersistence(setting, intpFactory, null);
    verify(intpFactory, times(1)).restart(any(String.class));
  }

  @Test
  public void persistentInterpreterWithBootstrap() {
    String noteId = "123";
    Properties properties = new Properties();
    properties.put(PersistentIntpsAndBootstrapNotes.getPeristentPropertyForUnitTest(), "true");
    properties.put(PersistentIntpsAndBootstrapNotes.getBootstrapNotebookForUnitTest(), noteId);

    InterpreterSetting setting = getSettingWithProperties(properties);
    InterpreterFactory intpFactory = mock(InterpreterFactory.class);
    List<Note> notes = new ArrayList<>();
    Note note = getBootStrapnote(true, noteId);
    notes.add(note);

    PersistentIntpsAndBootstrapNotes.checkPersistence(setting, intpFactory, notes);

    verify(intpFactory, times(0)).restart(any(String.class));
    verify(note, times(1)).runParagraphsForBootstrap(any(InterpreterSetting.class),
        any(String.class), any(String.class));
  }

  @Test
  public void persistentInterpreterWithBootstrapButNoSparkParagraphRun() {
    String noteId = "123";
    Properties properties = new Properties();
    properties.put(PersistentIntpsAndBootstrapNotes.getPeristentPropertyForUnitTest(), "true");
    properties.put(PersistentIntpsAndBootstrapNotes.getBootstrapNotebookForUnitTest(), noteId);

    InterpreterSetting setting = getSettingWithProperties(properties);

    InterpreterFactory intpFactory = mock(InterpreterFactory.class);
    List<Note> notes = new ArrayList<>();
    Note note = getBootStrapnote(false, noteId);
    notes.add(note);

    PersistentIntpsAndBootstrapNotes.checkPersistence(setting, intpFactory, notes);

    verify(intpFactory, times(1)).restart(any(String.class));
    verify(note, times(1)).runParagraphsForBootstrap(any(InterpreterSetting.class),
        any(String.class), any(String.class));

  }

  private static InterpreterSetting getSettingWithProperties(Properties properties) {
    Interpreter intp = mock(Interpreter.class);
    when(intp.getProperty()).thenReturn(properties);
    InterpreterGroup intpGroup = new InterpreterGroup();
    List<Interpreter> list = new ArrayList<>();
    InterpreterFactory fact = mock(InterpreterFactory.class);
    when(fact.createInterpreterGroup(any(String.class), any(InterpreterOption.class))).thenReturn(intpGroup);
    list.add(intp);
    intpGroup.put("shared_session", list);
    InterpreterSetting setting = new InterpreterSetting(
        null, QuboleUtil.sparkInterpreterGroupName, null, properties, null, null);
    setting.setInterpreterGroupFactory(fact);
    return setting;

  }

  private Note getBootStrapnote(boolean ranSparkParagraph, String noteId) {
    Note note = mock(Note.class);
    when(note.getQuboleNoteAttributes()).thenReturn(new QuboleNoteAttributes(
        null, null, new Integer(noteId), null));
    when(note.runParagraphsForBootstrap(any(InterpreterSetting.class),
        any(String.class), any(String.class))).thenReturn(ranSparkParagraph);
    return note;
  }
}
