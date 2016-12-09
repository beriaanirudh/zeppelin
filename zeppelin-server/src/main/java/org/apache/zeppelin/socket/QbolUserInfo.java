package org.apache.zeppelin.socket;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Purpose of this class is to un-complicate QuboleACLHelper
 *  by delegating the Maps of Maps concept.
 */
public class QbolUserInfo implements Map<String, Object> {

  private Map<String, Object> map;

  public QbolUserInfo(Map<String, Object> map) {
    this.map = map;
  }

  @Override
  public Set<String> keySet() {
    return new HashSet<String> (map.keySet());
  }

  @Override
  public Object get(Object key) {
    return map.get((String) key);
  }

  @Override
  public Object remove(Object key) {
    return map.remove((String) key);
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey((String) key);
  }

  @Override
  public Object put(String key, Object value) {
    return map.put(key, value);
  }

  public void removeConn(NotebookSocket conn) {
    for (String qbolUserId: map.keySet()) {
      Set<NotebookSocket> conns = (Set<NotebookSocket>) map.get(qbolUserId);
      conns.remove(conn);
    }
  }

  public void addConnection(String qbolUserId, NotebookSocket conn) {
    Set<NotebookSocket> set = (Set<NotebookSocket>) map.get(qbolUserId);
    set.add(conn);
  }


  // All other methods are not used. 
  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }


  @Override
  public boolean containsValue(Object value) {
    return false;
  }

  @Override
  public void putAll(Map<? extends String, ? extends Object> m) {
    
  }

  @Override
  public void clear() {
    
  }

  @Override
  public Collection<Object> values() {
    return null;
  }

  @Override
  public Set<java.util.Map.Entry<String, Object>> entrySet() {
    return null;
  }

}
