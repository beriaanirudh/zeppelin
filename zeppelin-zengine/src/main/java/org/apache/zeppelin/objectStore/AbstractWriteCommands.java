package org.apache.zeppelin.objectStore;

/**
 * 
 * @author Abstract class for objectstore write operations
 *
 */
public abstract class AbstractWriteCommands extends AbstractCommand {

  @Override
  public boolean validate() {
    boolean valid = super.validate();

    valid &= Utils.isValidPath(getDestination());
    return valid;
  }

}
