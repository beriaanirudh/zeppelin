package org.apache.zeppelin.objectStore;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author CommandManager
 *
 */

public class CommandManager {

  static {
    LOG = LoggerFactory.getLogger(CommandManager.class);
    mgr = new CommandManager();
  }

  private static final Logger LOG;
  private static CommandManager mgr;
  private ExecutorService service;

  private CommandManager() {
    // singleton. Preventing any other initializations
    // We can also configure number of threads based on machine's memory.
    ZeppelinConfiguration conf = ZeppelinConfiguration.create();
    int poolSize = conf.getSyncPoolSize();
    LOG.debug("Using Thread Pool Size: " + poolSize);
    service = Executors.newFixedThreadPool(10);
  }

  public static CommandManager getInstance() {
    return mgr;
  }

  public Future<Integer> executeCommand(final Command cmd) {
    Future<Integer> future = service.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        Integer returnVal = -1;
        LOG.debug("Running Command: " + cmd.getCommand());
        try {
          returnVal = cmd.execute();
        } catch (Exception e) {
          LOG.error("Error in running command: " + cmd.getCommand(), e);
        }
        return returnVal;
      }
    });
    return future;
  }

  public Command createCommand(ObjectStoreOperationsEnum cmdType) {
    switch (cmdType) {
        case GET:
          return new GetCommand();
        case PUT:
          return new PutCommand();
        case SYNC:
          return new SyncCommand();
    }
    return null;
  }
}
