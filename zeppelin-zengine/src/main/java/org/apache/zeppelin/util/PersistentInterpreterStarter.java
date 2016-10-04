package org.apache.zeppelin.util;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zeppelin.interpreter.Interpreter;

/**
 * This class starts the interpreters in async
 * thread since we donot want UI to be hung while
 * interpreter creation/update, or starting zeppelin-server
 * to take lot of time un-necessary.
 */
public class PersistentInterpreterStarter implements Runnable {

  private static final ExecutorService service = Executors.newCachedThreadPool();
  private static final Set<Interpreter> openingInterpreters = new HashSet<>();
  private final Interpreter interpreter;

  private PersistentInterpreterStarter(Interpreter interpreter) {
    this.interpreter = interpreter;
  }

  @Override
  public void run() {
    /* Although LazyOpenInterpreter handles synchronization
     * on the remote interpreter end, we try not to pro-actively
     * open an interpreter multiple times simultaneously as
     * it would waste resources.
     */
    synchronized (openingInterpreters) {
      if (openingInterpreters.contains(interpreter)) {
        return;
      }
      openingInterpreters.add(interpreter);
    }
    interpreter.open();
    interpreter.openInterpreterProactively();
    synchronized (openingInterpreters) {
      openingInterpreters.remove(interpreter);
    }
  }

  public static void startPersistentInterpreter(Interpreter interpreter) {
    service.execute(new PersistentInterpreterStarter(interpreter));
  }
}
