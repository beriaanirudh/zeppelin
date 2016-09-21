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

package org.apache.zeppelin.interpreter;

import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;

/**
 * Interpreter wrapper for lazy initialization
 */
public class LazyOpenInterpreter
    extends Interpreter
    implements WrappedInterpreter {
  private Interpreter intp;
  boolean opened = false;

  public LazyOpenInterpreter(Interpreter intp) {
    super(new Properties());
    this.intp = intp;
  }

  @Override
  public Interpreter getInnerInterpreter() {
    return intp;
  }

  @Override
  public void setProperty(Properties property) {
    intp.setProperty(property);
  }

  @Override
  public Properties getProperty() {
    return intp.getProperty();
  }

  @Override
  public String getProperty(String key) {
    return intp.getProperty(key);
  }

  @Override
  public void open() {
    if (opened == true) {
      return;
    }

    synchronized (intp) {
      if (opened == false) {
        intp.open();
        opened = true;
      }
    }
  }

  @Override
  public void close() {
    synchronized (intp) {
      if (opened == true) {
        intp.close();
        opened = false;
      }
    }
  }

  public boolean isOpen() {
    synchronized (intp) {
      return opened;
    }
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    try {
      open();
    } catch (Throwable e) {
      //This try catch is so that we can add "Error happened while creating interpreter."
      //to exception message. Catching proper exceptions in interpret are necessary becoz
      //these get shown on UI
      throw new InterpreterException("Error happened while creating interpreter. Error: " + 
          e.getMessage(), e); 
    }
    return intp.interpret(st, context);
  }

  @Override
  public void cancel(InterpreterContext context) {
    try {
      open();
    } catch (Throwable e) {
      //code for exception catching in cancel, getProgress and completion is not stictly reqd.
      //Added it just to keep logs clean. open function throws an exception if it fails 
      //to create sc. reason for failure to create sc can be some wrong parameter
      return;
    }
    intp.cancel(context);
  }

  @Override
  public FormType getFormType() {
    return intp.getFormType();
  }

  @Override
  public int getProgress(InterpreterContext context) {
    try {
      open();
    } catch (Throwable e) {
      //Just report 0 as progress if we fail to create sc
      return 0;
    }
    return intp.getProgress(context);
  }

  @Override
  public Scheduler getScheduler() {
    return intp.getScheduler();
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    try {
      open();
      List completion = intp.completion(buf, cursor);
      return completion;
    } catch (Throwable e) {
      //Return emtry array for completion if we fail to create sc
      return new ArrayList<InterpreterCompletion>();
    }
  }

  @Override
  public String getClassName() {
    return intp.getClassName();
  }

  @Override
  public InterpreterGroup getInterpreterGroup() {
    return intp.getInterpreterGroup();
  }

  @Override
  public void setInterpreterGroup(InterpreterGroup interpreterGroup) {
    intp.setInterpreterGroup(interpreterGroup);
  }

  @Override
  public URL [] getClassloaderUrls() {
    return intp.getClassloaderUrls();
  }

  @Override
  public void setClassloaderUrls(URL [] urls) {
    intp.setClassloaderUrls(urls);
  }

  @Override
  public void openInterpreterProactively() {
    intp.openInterpreterProactively();
  }
}
