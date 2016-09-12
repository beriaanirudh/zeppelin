package org.apache.zeppelin.s3;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author S3commandManager
 *
 */

public class S3CommandManager {

  static {
    mgr = new S3CommandManager();
  }

  private static final Logger LOG = LoggerFactory.getLogger(S3CommandManager.class);
  private static S3CommandManager mgr;
  private ExecutorService service;

  private S3CommandManager() {
    // singleton. Preventing any other initializations
    service = Executors.newCachedThreadPool();
  }

  public static S3CommandManager getInstance() {
    return mgr;
  }

  public Future<Integer> executeCommand(final S3Command cmd) {
    Future<Integer> future = service.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        Integer returnVal = -1;
        try {
          returnVal = cmd.execute();
        } catch (Exception e) {
          LOG.error("s3ops S3 operation failed. command: " + cmd.getCommand());
        }
        return returnVal;
      }
    });
    return future;
  }

  public S3Command createCommand(S3OperationsEnum cmdType) {
    switch (cmdType) {
        case GET:
          return new S3GetCommand();
        case PUT:
          return new S3PutCommand();
        case SYNC:
          return new S3SyncCommand();
    }
    return null;
  }
}
