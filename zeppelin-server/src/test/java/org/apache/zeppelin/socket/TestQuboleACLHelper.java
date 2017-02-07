package org.apache.zeppelin.socket;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.NotebookAuthorization;
import org.apache.zeppelin.util.QuboleNoteAttributes;
import org.apache.zeppelin.util.QuboleUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.gson.Gson;

/** 
 * Compiler dont cry
 * @author beria
 *
 */
@PrepareForTest(QuboleUtil.class)
@RunWith(PowerMockRunner.class)
public class TestQuboleACLHelper {

  String zeppelinId;
  String qbolUserId;
  Integer noteId;
  Notebook notebook;
  Note note;
  QuboleNoteAttributes attrs;
  NotebookAuthorization auth;
  HttpServletRequest request;

  @Before
  public void beforeTests() {
    PowerMockito.mockStatic(QuboleUtil.class);
    zeppelinId = "random-id";
    qbolUserId = "123";
    noteId = 1;

    notebook = mock(Notebook.class);
    note = mock(Note.class);
    attrs = new QuboleNoteAttributes(qbolUserId, null, noteId, null);
    auth = mock(NotebookAuthorization.class);
    request = mock(HttpServletRequest.class);

    when(notebook.getNote(zeppelinId)).thenReturn(note);
    when(note.getQuboleNoteAttributes()).thenReturn(attrs);
    when(notebook.getNotebookAuthorization()).thenReturn(auth);
    when(request.getHeader(QuboleServerHelper.QBOL_USER_ID)).thenReturn(qbolUserId);

  }

  @Test
  public void testIsOperationAllowed() {
    Set<String> entity = new HashSet<>();
    entity.add(qbolUserId);
    when(auth.isReader(zeppelinId, entity)).thenReturn(true);

    String res = "[{\"qbolUserId\":\"" + qbolUserId + "\",\"zeppelinId\":\""
        + zeppelinId + "\",\"permissions\":{\"read\":true}}]";
    try {
      when(QuboleUtil.getResponseFromConnection(any(HttpURLConnection.class))).thenReturn(res);
    } catch (Exception e) {
    }

    boolean allowed = QuboleACLHelper.isOperationAllowed(zeppelinId, request, notebook, QuboleACLHelper.Operation.READ);
    assertEquals(allowed, true);
    verify(auth, times(1)).addPermissionForQubole(zeppelinId, qbolUserId, "readers");
  }

  @Test
  public void testCanCreateNote() {
    when(request.getHeader(QuboleServerHelper.QBOL_USER_ID)).thenReturn(qbolUserId);
    String res = "[{\"qbolUserId\":\"" + qbolUserId + "\", \"permissions\":{\"create\":true}}]";
    try {
      when(QuboleUtil.getResponseFromConnection(any(HttpURLConnection.class))).thenReturn(res);
    } catch (Exception e) {
    }

    Boolean allowed = QuboleACLHelper.canCreateNote(request);
    assertEquals(allowed, true);
  }

  /* We test 2 things in refresh:
   * 1. If there is no live connection (NotebookSocket), we should not
   *    un-necessarily refresh ACL for that.
   * 2. Calls are batched. Since default batch size is 20 and 24 permissions
   *    need to be fetched, we should see 2 calls to tapp.
   */
  @Test
  public void testRefreshACLs() {
    int numNewPermissions = 25;
    List<Map<String, Object>> finalList = new ArrayList<>();
    Gson gson = new Gson();

    for (int i = 0; i < numNewPermissions; i++) {
      NotebookSocket conn = null;
      /* for qbolUserId = 0, there will be no connection. Hence, this will not
       * be refreshed when need be. */
      if (i != 0) {
        conn = mock(NotebookSocket.class);
      }

      String qbolUserId2 = "" + i;
      HttpServletRequest request2 = mock(HttpServletRequest.class);
      when(request2.getHeader(QuboleServerHelper.QBOL_USER_ID)).thenReturn(qbolUserId2);

      List<Map<String, Object>> list = new ArrayList();
      Map<String, Object> result = new HashMap<>();
      result.put("qbolUserId", qbolUserId2);
      result.put("zeppelinId", zeppelinId);
      Map<String, Boolean> perms = new HashMap<>();
      perms.put("read", true);
      result.put("permissions", perms);
      list.add(result);
      finalList.add(result);
      try {
        when(QuboleUtil.getResponseFromConnection(any(HttpURLConnection.class))).thenReturn(gson.toJson(list));
      } catch (Exception e) {
      }
      QuboleACLHelper.checkAndAddNotebookACL(notebook, qbolUserId2, zeppelinId, conn);
    }

    // for all numNewPermissions permissions, calls are made to tapp initially. 
    verifyStatic(Mockito.times(numNewPermissions)); QuboleUtil.getPermissions(any(List.class));

    try {
      when(QuboleUtil.getResponseFromConnection(any(HttpURLConnection.class))).thenReturn(gson.toJson(finalList));
    } catch (Exception e1) {
    }

    QuboleACLHelper.getRunnableForUnitTests(notebook).run();

    // 2 more calls to tapp (here 24 permissions need to be fetched)
    verifyStatic(Mockito.times(numNewPermissions + 2)); QuboleUtil.getPermissions(any(List.class));
    /* permission for qbolUserId = 0 should be removed since there was no connection
     * and hence was not refreshed.
     */
    verify(auth, times(1)).removePermissionForQubole(zeppelinId, "0", "readers");
  }
}
