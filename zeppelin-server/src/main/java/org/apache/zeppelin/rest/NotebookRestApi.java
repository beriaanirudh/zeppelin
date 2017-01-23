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

package org.apache.zeppelin.rest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.annotation.ZeppelinApi;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zeppelin.events.QuboleEventUtils;
import org.apache.zeppelin.events.QuboleEventsEnum.EVENTTYPE;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.NotebookAuthorization;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.rest.message.CronRequest;
import org.apache.zeppelin.rest.message.InterpreterSettingListForNoteBind;
import org.apache.zeppelin.rest.message.NewNotebookRequest;
import org.apache.zeppelin.rest.message.NewParagraphRequest;
import org.apache.zeppelin.rest.message.RunParagraphWithParametersRequest;
import org.apache.zeppelin.search.SearchService;
import org.apache.zeppelin.rest.message.NewParagraphRunRequest;
import org.apache.zeppelin.rest.message.RunNotebookResponse;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.socket.NotebookServer;
import org.apache.zeppelin.socket.QuboleServerHelper;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.socket.QuboleACLHelper;
import org.apache.zeppelin.utils.SecurityUtils;
import org.quartz.CronExpression;
import org.apache.zeppelin.server.ZeppelinServer;
import org.apache.zeppelin.util.QuboleNoteAttributes;
import org.apache.zeppelin.util.QuboleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;
import com.google.gson.GsonBuilder;


import static org.apache.zeppelin.socket.QuboleACLHelper.Operation.READ;
import static org.apache.zeppelin.socket.QuboleACLHelper.Operation.WRITE;
import static org.apache.zeppelin.socket.QuboleACLHelper.Operation.DELETE;

/**
 * Rest api endpoint for the noteBook.
 */
@Path("/notebook")
@Produces("application/json")
public class NotebookRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(NotebookRestApi.class);

  Gson gson = new Gson();
  private Notebook notebook;
  private NotebookServer notebookServer;
  private SearchService notebookIndex;
  private NotebookAuthorization notebookAuthorization;

  public NotebookRestApi() {}

  public NotebookRestApi(Notebook notebook, NotebookServer notebookServer, SearchService search) {
    this.notebook = notebook;
    this.notebookServer = notebookServer;
    this.notebookIndex = search;
    this.notebookAuthorization = notebook.getNotebookAuthorization();
  }

  /**
   * get note authorization information
   */
  @GET
  @Path("{noteId}/permissions")
  @ZeppelinApi
  public Response getNotePermissions(@PathParam("noteId") String noteId) {
    /*
    Note note = notebook.getNote(noteId);
    HashMap<String, Set<String>> permissionsMap = new HashMap<>();
    permissionsMap.put("owners", notebookAuthorization.getOwners(noteId));
    permissionsMap.put("readers", notebookAuthorization.getReaders(noteId));
    permissionsMap.put("writers", notebookAuthorization.getWriters(noteId));
    return new JsonResponse<>(Status.OK, "", permissionsMap).build();
  }

  String ownerPermissionError(Set<String> current,
                              Set<String> allowed) throws IOException {
    LOG.info("Cannot change permissions. Connection owners {}. Allowed owners {}",
            current.toString(), allowed.toString());
    return "Insufficient privileges to change permissions.\n\n" +
           "Allowed owners: " + allowed.toString() + "\n\n" +
           "User belongs to: " + current.toString();
  }
    */
    return new JsonResponse<>(Status.FORBIDDEN).build();
  }

  /**
   * set note authorization information
   */
  @PUT
  @Path("{noteId}/permissions")
  @ZeppelinApi
  public Response putNotePermissions(@PathParam("noteId") String noteId, String req)
      throws IOException {
    /**
     * TODO(jl): Fixed the type of HashSet
     * https://issues.apache.org/jira/browse/ZEPPELIN-1162
     *
    HashMap<String, HashSet<String>> permMap = gson.fromJson(req,
            new TypeToken<HashMap<String, HashSet<String>>>() {}.getType());
    Note note = notebook.getNote(noteId);
    String principal = SecurityUtils.getPrincipal();
    HashSet<String> roles = SecurityUtils.getRoles();
    LOG.info("Set permissions {} {} {} {} {}",
            noteId,
            principal,
            permMap.get("owners"),
            permMap.get("readers"),
            permMap.get("writers"));

    HashSet<String> userAndRoles = new HashSet<String>();
    userAndRoles.add(principal);
    userAndRoles.addAll(roles);
    if (!notebookAuthorization.isOwner(noteId, userAndRoles)) {
      return new JsonResponse<>(Status.FORBIDDEN, ownerPermissionError(userAndRoles,
              notebookAuthorization.getOwners(noteId))).build();
    }

    HashSet<> readers = permMap.get("readers");
    HashSet<> owners = permMap.get("owners");
    HashSet<> writers = permMap.get("writers");
    // Set readers, if writers and owners is empty -> set to user requesting the change
    if (readers != null && !readers.isEmpty()) {
      if (writers.isEmpty()) {
        writers = Sets.newHashSet(SecurityUtils.getPrincipal());
      }
      if (owners.isEmpty()) {
        owners = Sets.newHashSet(SecurityUtils.getPrincipal());
      }
    }
    // Set writers, if owners is empty -> set to user requesting the change
    if ( writers != null && !writers.isEmpty()) {
      if (owners.isEmpty()) {
        owners = Sets.newHashSet(SecurityUtils.getPrincipal());
      }
    }

    notebookAuthorization.setReaders(noteId, readers);
    notebookAuthorization.setWriters(noteId, writers);
    notebookAuthorization.setOwners(noteId, owners);
    LOG.debug("After set permissions {} {} {}",
            notebookAuthorization.getOwners(noteId),
            notebookAuthorization.getReaders(noteId),
            notebookAuthorization.getWriters(noteId));
    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    note.persist(subject);
    notebookServer.broadcastNote(note);
    return new JsonResponse<>(Status.OK).build();
    */
    return new JsonResponse<>(Status.FORBIDDEN).build();
  }

  /**
   * bind a setting to note
   * @throws IOException
   */
  @PUT
  @Path("interpreter/bind/{noteId}")
  @ZeppelinApi
  public Response bind(@PathParam("noteId") String noteId, String req) throws IOException {
    List<String> settingIdList = gson.fromJson(req, new TypeToken<List<String>>(){}.getType());
    notebook.bindInterpretersToNote(noteId, settingIdList);
    return new JsonResponse<>(Status.OK).build();
  }

  /**
   * list binded setting
   */
  @GET
  @Path("interpreter/bind/{noteId}")
  @ZeppelinApi
  public Response bind(@PathParam("noteId") String noteId) {
    List<InterpreterSettingListForNoteBind> settingList
      = new LinkedList<InterpreterSettingListForNoteBind>();

    List<InterpreterSetting> selectedSettings = notebook.getBindedInterpreterSettings(noteId);
    for (InterpreterSetting setting : selectedSettings) {
      settingList.add(new InterpreterSettingListForNoteBind(
          setting.id(),
          setting.getName(),
          setting.getGroup(),
          setting.getInterpreterInfos(),
          true)
      );
    }

    List<InterpreterSetting> availableSettings = notebook.getInterpreterFactory().get();
    for (InterpreterSetting setting : availableSettings) {
      boolean selected = false;
      for (InterpreterSetting selectedSetting : selectedSettings) {
        if (selectedSetting.id().equals(setting.id())) {
          selected = true;
          break;
        }
      }

      if (!selected) {
        settingList.add(new InterpreterSettingListForNoteBind(
            setting.id(),
            setting.getName(),
            setting.getGroup(),
            setting.getInterpreterInfos(),
            false)
        );
      }
    }
    return new JsonResponse<>(Status.OK, "", settingList).build();
  }

  @GET
  @Path("/")
  @ZeppelinApi
  public Response getNotebookList() throws IOException {
    return new JsonResponse<>(Status.FORBIDDEN).build();
    /*
    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    List<Map<String, String>> notesInfo = notebookServer.generateNotebooksInfo(false, subject);
    return new JsonResponse<>(Status.OK, "", notesInfo ).build();
    */
  }

  @GET
  @Path("{notebookId}")
  @ZeppelinApi
  public Response getNotebook(@Context HttpServletRequest request,
                              @PathParam("notebookId") String notebookId) throws IOException {
    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }

    if (!QuboleACLHelper.isOperationAllowed(notebookId, request, notebook, READ)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    return new JsonResponse<>(Status.OK, "", note).build();
  }

  /**
   * export note REST API
   * 
   * @param
   * @return note JSON with status.OK
   * @throws IOException
   */
  @GET
  @Path("export/{id}")
  @ZeppelinApi
  public Response exportNoteBook(@Context HttpServletRequest request,
                                 @PathParam("id") String noteId) throws IOException {
    if (!QuboleACLHelper.isOperationAllowed(noteId, request, notebook, READ)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }
    String exportJson = notebook.exportNote(noteId);
    return new JsonResponse(Status.OK, "", exportJson).build();
  }

  @GET
  @Path("note/fetch/{noteId}")
  public Response fetch(@Context HttpServletRequest request,
      @PathParam("noteId") String noteId) {
    return QuboleServerHelper.fetch(request, notebook, noteId);
  }

  /**
   * import new note REST API
   * 
   * @param req - notebook Json
   * @return JSON with new note ID
   * @throws IOException
   */
  @POST
  @Path("import")
  @ZeppelinApi
  public Response importNotebook(@Context HttpServletRequest request,
                                  String req) throws IOException {
    if (!QuboleACLHelper.canCreateNote(request)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    Note newNote = notebook.importNote(req, null, subject);
    return new JsonResponse<>(Status.CREATED, "", newNote.getId()).build();
  }
  
  /**
   * Create new note REST API
   * @param message - JSON with new note name
   * @return JSON with new note ID
   * @throws IOException
   */
  @POST
  @Path("/")
  @ZeppelinApi
  public Response createNote(@Context HttpServletRequest req, String message)
      throws IOException {
    if (!QuboleACLHelper.canCreateNote(req)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    LOG.info("Create new notebook by JSON {}" , message);
    NewNotebookRequest request = gson.fromJson(message,
        NewNotebookRequest.class);
    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    Note note = notebook.createNote(subject);
    List<NewParagraphRequest> initialParagraphs = request.getParagraphs();
    if (initialParagraphs != null) {
      for (NewParagraphRequest paragraphRequest : initialParagraphs) {
        Paragraph p = note.addParagraph();
        p.setTitle(paragraphRequest.getTitle());
        p.setText(paragraphRequest.getText());
      }
    }
    note.addParagraph(); // add one paragraph to the last
    String noteName = request.getName();
    if (noteName.isEmpty()) {
      noteName = "Note " + note.getId();
    }

    note.setName(noteName);
    note.persist(subject);
    notebookServer.broadcastNote(note);
    notebookServer.broadcastNoteList(subject);
    return new JsonResponse<>(Status.CREATED, "", note.getId() ).build();
  }

  @PUT
  @Path("note/checkout/{noteId}")
  public Response checkout(@Context HttpServletRequest request,
      @PathParam("noteId") String noteId, String data) {
    return QuboleServerHelper.checkout(notebook, noteId, data, request);
  }

  /**
   * Delete note REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException
   */
  @DELETE
  @Path("note/{notebookId}")
  @ZeppelinApi
  public Response deleteNote(@Context HttpServletRequest request,
                             @PathParam("notebookId") String notebookId) throws IOException {
    if (!QuboleACLHelper.isOperationAllowed(notebookId, request, notebook, DELETE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    LOG.info("Delete notebook {} ", notebookId);
    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    if (!(notebookId.isEmpty())) {
      Note note = notebook.getNote(notebookId);
      if (note != null) {
        notebook.removeNote(notebookId, subject);
      }
    }

    QuboleACLHelper.onNoteDelete(notebookId);
    notebookServer.broadcastNoteList(subject);
    return new JsonResponse<>(Status.OK, "").build();
  }
  
  /**
   * Clone note REST API
   * @param
   * @return JSON with status.CREATED
   * @throws IOException, CloneNotSupportedException, IllegalArgumentException
   */
  @POST
  @Path("{notebookId}")
  @ZeppelinApi
  public Response cloneNote(@Context HttpServletRequest req,
                            @PathParam("notebookId") String notebookId, String message) throws
      IOException, CloneNotSupportedException, IllegalArgumentException {
    if (!QuboleACLHelper.isOperationAllowed(notebookId, req, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    LOG.info("clone notebook by JSON {}" , message);
    NewNotebookRequest request = gson.fromJson(message,
        NewNotebookRequest.class);
    String newNoteName = request.getName();
    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    Note newNote = notebook.cloneNote(notebookId, newNoteName, subject);
    notebookServer.broadcastNote(newNote);
    notebookServer.broadcastNoteList(subject);
    return new JsonResponse<>(Status.CREATED, "", newNote.getId()).build();
  }

  /**
   * Insert paragraph REST API
   * @param message - JSON containing paragraph's information
   * @return JSON with status.OK
   * @throws IOException
   */
  @POST
  @Path("{notebookId}/paragraph")
  @ZeppelinApi
  public Response insertParagraph(@Context HttpServletRequest req,
                                  @PathParam("notebookId") String notebookId, String message)
      throws IOException {
    if (!QuboleACLHelper.isOperationAllowed(notebookId, req, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }
    LOG.info("insert paragraph {} {}", notebookId, message);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "note not found.").build();
    }

    NewParagraphRequest request = gson.fromJson(message, NewParagraphRequest.class);

    Paragraph p;
    Double indexDouble = request.getIndex();
    if (indexDouble == null) {
      p = note.addParagraph();
    } else {
      p = note.insertParagraph(indexDouble.intValue());
    }
    p.setTitle(request.getTitle());
    p.setText(request.getText());

    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    note.persist(subject);
    notebookServer.broadcastNote(note);
    return new JsonResponse(Status.CREATED, "", p.getId()).build();
  }

  /**
   * Get paragraph REST API
   * @param
   * @return JSON with information of the paragraph
   * @throws IOException
   */
  @GET
  @Path("{notebookId}/paragraph/{paragraphId}")
  @ZeppelinApi
  public Response getParagraph(@Context HttpServletRequest request,
                               @PathParam("notebookId") String notebookId,
                               @PathParam("paragraphId") String paragraphId) throws IOException {
    if (!QuboleACLHelper.isOperationAllowed(notebookId, request, notebook, READ)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    LOG.info("get paragraph {} {}", notebookId, paragraphId);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      return new JsonResponse(Status.NOT_FOUND, "paragraph not found.").build();
    }

    return new JsonResponse(Status.OK, "", p).build();
  }

  /**
   * Move paragraph REST API
   * @param newIndex - new index to move
   * @return JSON with status.OK
   * @throws IOException
   */
  @POST
  @Path("{notebookId}/paragraph/{paragraphId}/move/{newIndex}")
  @ZeppelinApi
  public Response moveParagraph(@Context HttpServletRequest request,
                                @PathParam("notebookId") String notebookId,
                                @PathParam("paragraphId") String paragraphId,
                                @PathParam("newIndex") String newIndex) throws IOException {
    if (!QuboleACLHelper.isOperationAllowed(notebookId, request, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    LOG.info("move paragraph {} {} {}", notebookId, paragraphId, newIndex);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      return new JsonResponse(Status.NOT_FOUND, "paragraph not found.").build();
    }

    try {
      note.moveParagraph(paragraphId, Integer.parseInt(newIndex), true);

      AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
      note.persist(subject);
      notebookServer.broadcastNote(note);
      return new JsonResponse(Status.OK, "").build();
    } catch (IndexOutOfBoundsException e) {
      LOG.error("Exception in NotebookRestApi while moveParagraph ", e);
      return new JsonResponse(Status.BAD_REQUEST, "paragraph's new index is out of bound").build();
    }
  }

  /**
   * Delete paragraph REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException
   */
  @DELETE
  @Path("{notebookId}/paragraph/{paragraphId}")
  @ZeppelinApi
  public Response deleteParagraph(@Context HttpServletRequest request,
                                  @PathParam("notebookId") String notebookId,
                                  @PathParam("paragraphId") String paragraphId) throws IOException {
    if (!QuboleACLHelper.isOperationAllowed(notebookId, request, notebook, DELETE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    LOG.info("delete paragraph {} {}", notebookId, paragraphId);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      return new JsonResponse(Status.NOT_FOUND, "paragraph not found.").build();
    }

    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    note.removeParagraph(paragraphId);
    note.persist(subject);
    notebookServer.broadcastNote(note);

    return new JsonResponse(Status.OK, "").build();
  }

  /**
   * Run notebook jobs REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @POST
  @Path("job/{notebookId}")
  @ZeppelinApi
  public Response runNoteJobs(@Context HttpServletRequest request,
                              @PathParam("notebookId") String notebookId) throws
      IOException, IllegalArgumentException {
    if (!QuboleACLHelper.isOperationAllowed(notebookId, request, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }
    LOG.info("run notebook jobs {} ", notebookId);
    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }

    try {
      note.runAll();
    } catch (Exception ex) {
      LOG.error("Exception from run", ex);
      return new JsonResponse<>(Status.PRECONDITION_FAILED,
          ex.getMessage() + "- Not selected or Invalid Interpreter bind").build();
    }

    return new JsonResponse<>(Status.OK).build();
  }

  /**
   * Stop(delete) notebook jobs REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @DELETE
  @Path("job/{notebookId}")
  @ZeppelinApi
  public Response stopNoteJobs(@Context HttpServletRequest request,
                               @PathParam("notebookId") String notebookId) throws
      IOException, IllegalArgumentException {
    LOG.info("stop notebook jobs {} ", notebookId);
    if (!QuboleACLHelper.isOperationAllowed(notebookId, request, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }

    for (Paragraph p : note.getParagraphs()) {
      if (!p.isTerminated()) {
        p.abort();
      }
    }
    return new JsonResponse<>(Status.OK).build();
  }
  
  /**
   * Get notebook job status REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @GET
  @Path("job/{notebookId}")
  @ZeppelinApi
  public Response getNoteJobStatus(@Context HttpServletRequest request,
                                   @PathParam("notebookId") String notebookId) throws
      IOException, IllegalArgumentException {
    LOG.info("get notebook job status.");
    if (!QuboleACLHelper.isOperationAllowed(notebookId, request, notebook, READ)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }
    return QuboleServerHelper.conformResponseForTapp(note);
  }

  /**
   * Run paragraph job REST API
   * 
   * @param message - JSON with params if user wants to update dynamic form's value
   *                null, empty string, empty json if user doesn't want to update
   *
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @POST
  @Path("job/{notebookId}/{paragraphId}")
  @ZeppelinApi
  public Response runParagraph(@Context HttpServletRequest req,
                               @PathParam("notebookId") String notebookId, 
                               @PathParam("paragraphId") String paragraphId,
                               String message) throws
                               IOException, IllegalArgumentException {
    LOG.info("run paragraph job {} {} {}", notebookId, paragraphId, message);
    if (!QuboleACLHelper.isOperationAllowed(notebookId, req, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }


    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph paragraph = note.getParagraph(paragraphId);
    if (paragraph == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "paragraph not found.").build();
    }

    // handle params if presented
    if (!StringUtils.isEmpty(message)) {
      RunParagraphWithParametersRequest request = gson.fromJson(message,
          RunParagraphWithParametersRequest.class);
      Map<String, Object> paramsForUpdating = request.getParams();
      if (paramsForUpdating != null) {
        paragraph.settings.getParams().putAll(paramsForUpdating);
        AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
        note.persist(subject);
      }
    }

    note.run(paragraph.getId());
    return new JsonResponse<>(Status.OK).build();
  }

  /**
   * Stop(delete) paragraph job REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @DELETE
  @Path("job/{notebookId}/{paragraphId}")
  @ZeppelinApi
  public Response stopParagraph(@Context HttpServletRequest req,
                                @PathParam("notebookId") String notebookId, 
                                @PathParam("paragraphId") String paragraphId) throws
                                IOException, IllegalArgumentException {
    LOG.info("stop paragraph job {} ", notebookId);
    if (!QuboleACLHelper.isOperationAllowed(notebookId, req, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }
    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "paragraph not found.").build();
    }
    p.abort();
    return new JsonResponse<>(Status.OK).build();
  }
    
  /**
   * Register cron job REST API
   * @param message - JSON with cron expressions.
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @POST
  @Path("cron/{notebookId}")
  @ZeppelinApi
  public Response registerCronJob(@Context HttpServletRequest req,
                                  @PathParam("notebookId") String notebookId, String message) throws
      IOException, IllegalArgumentException {
    LOG.info("Register cron job note={} request cron msg={}", notebookId, message);
    if (!QuboleACLHelper.isOperationAllowed(notebookId, req, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    CronRequest request = gson.fromJson(message,
                          CronRequest.class);
    
    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }
    
    if (!CronExpression.isValidExpression(request.getCronString())) {
      return new JsonResponse<>(Status.BAD_REQUEST, "wrong cron expressions.").build();
    }

    Map<String, Object> config = note.getConfig();
    config.put("cron", request.getCronString());
    note.setConfig(config);
    notebook.refreshCron(note.id());
    
    return new JsonResponse<>(Status.OK).build();
  }
  
  /**
   * Remove cron job REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @DELETE
  @Path("cron/{notebookId}")
  @ZeppelinApi
  public Response removeCronJob(@Context HttpServletRequest req,
                                @PathParam("notebookId") String notebookId) throws
      IOException, IllegalArgumentException {
    LOG.info("Remove cron job note {}", notebookId);

    if (!QuboleACLHelper.isOperationAllowed(notebookId, req, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }
    
    Map<String, Object> config = note.getConfig();
    config.put("cron", null);
    note.setConfig(config);
    notebook.refreshCron(note.id());
    
    return new JsonResponse<>(Status.OK).build();
  }  
  
  /**
   * Get cron job REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @GET
  @Path("cron/{notebookId}")
  @ZeppelinApi
  public Response getCronJob(@Context HttpServletRequest req,
                             @PathParam("notebookId") String notebookId) throws
      IOException, IllegalArgumentException {
    LOG.info("Get cron job note {}", notebookId);

    if (!QuboleACLHelper.isOperationAllowed(notebookId, req, notebook, READ)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }


    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }
    
    return new JsonResponse<>(Status.OK, note.getConfig().get("cron")).build();
  }

  /**
   * Get notebook jobs for job manager
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @GET
  @Path("jobmanager/")
  @ZeppelinApi
  public Response getJobListforNotebook() throws IOException, IllegalArgumentException {
    LOG.info("Get notebook jobs for job manager");

    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    List<Map<String, Object>> notebookJobs = notebook.getJobListforNotebook(false, 0, subject);
    Map<String, Object> response = new HashMap<>();

    response.put("lastResponseUnixTime", System.currentTimeMillis());
    response.put("jobs", notebookJobs);

    return new JsonResponse<>(Status.OK, response).build();
  }

  /**
   * Get updated notebook jobs for job manager
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @GET
  @Path("jobmanager/{lastUpdateUnixtime}/")
  @ZeppelinApi
  public Response getUpdatedJobListforNotebook(
      @PathParam("lastUpdateUnixtime") long lastUpdateUnixTime) throws
      IOException, IllegalArgumentException {
    LOG.info("Get updated notebook jobs lastUpdateTime {}", lastUpdateUnixTime);

    List<Map<String, Object>> notebookJobs;
    AuthenticationInfo subject = new AuthenticationInfo(SecurityUtils.getPrincipal());
    notebookJobs = notebook.getJobListforNotebook(false, lastUpdateUnixTime, subject);
    Map<String, Object> response = new HashMap<>();

    response.put("lastResponseUnixTime", System.currentTimeMillis());
    response.put("jobs", notebookJobs);

    return new JsonResponse<>(Status.OK, response).build();
  }

  /**
   * Search for a Notes with permissions
   */
  @GET
  @Path("search")
  @ZeppelinApi
  public Response search(@QueryParam("q") String queryTerm) {
    //should this be allowed?
    return new JsonResponse<>(Status.FORBIDDEN).build();
    /*
    LOG.info("Searching notebooks for: {}", queryTerm);
    String principal = SecurityUtils.getPrincipal();
    HashSet<String> roles = SecurityUtils.getRoles();
    HashSet<String> userAndRoles = new HashSet<String>();
    userAndRoles.add(principal);
    userAndRoles.addAll(roles);
    List<Map<String, String>> notebooksFound = notebookIndex.query(queryTerm);
    for (int i = 0; i < notebooksFound.size(); i++) {
      String[] Id = notebooksFound.get(i).get("id").split("/", 2);
      String noteId = Id[0];
      if (!notebookAuthorization.isOwner(noteId, userAndRoles) &&
              !notebookAuthorization.isReader(noteId, userAndRoles) &&
              !notebookAuthorization.isWriter(noteId, userAndRoles)) {
        notebooksFound.remove(i);
        i--;
      }
    }
    LOG.info("{} notebooks found", notebooksFound.size());
    return new JsonResponse<>(Status.OK, notebooksFound).build();
    */
  }

  @GET
  @Path("note/{noteId}")
  public Response getNote(@Context HttpServletRequest req, @PathParam("noteId") String noteId) {
    if (!QuboleACLHelper.isOperationAllowed(noteId, req, notebook, READ)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }
    if (noteId == null || noteId.trim().length() == 0) {
      return new JsonResponse(Status.NOT_FOUND, "", noteId).build();
    }
    Note note = notebook.getNote(noteId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "", noteId).build();
    }
    return new JsonResponse(Status.OK, "", note).build();
  }

  @POST
  @Path("note/{noteId}/paragraph")
  public Response createAndRunParagraph(@Context HttpServletRequest req,
                                        @PathParam("noteId") String noteId,
                                        String message) {
    if (!QuboleACLHelper.isOperationAllowed(noteId, req, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    NewParagraphRunRequest request = gson.fromJson(message, NewParagraphRunRequest.class);
    if (request == null) {
      return new JsonResponse(
          Status.BAD_REQUEST, "Request should contain body with paragraph text.").build();
    }
    String text = request.getParagraph();
    if (text == null || text.isEmpty()) {
      return new JsonResponse(Status.BAD_REQUEST, "Paragraph text should not be empty").build();
    }
    Note note = notebook.getNote(noteId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "Note not found:", noteId).build();
    }
    Paragraph p = note.addParagraph();
    p.setText(text);
    p.setTitle(request.getTitle());
    p.setConfig(request.getConfig());
    QuboleServerHelper.setQueryHistInParagraph(request, p, noteId);
    p.settings.setParams(request.getParams());

    try {
      LOG.info("Running paragraph: " + p.getId());
      note.run(p.getId());
    } catch (Exception ex) {
      LOG.error("Exception from run", ex);
      if (p != null) {
        p.setReturn(new InterpreterResult(InterpreterResult.Code.ERROR, ex.getMessage()), ex);
        p.setStatus(Job.Status.ERROR);
        return new JsonResponse(
            Status.INTERNAL_SERVER_ERROR,
            ex.getMessage(),
            ExceptionUtils.getStackTrace(ex)).build();
      }
    }
    return new JsonResponse(Status.CREATED, "", p).build();
  }

  @PUT
  @Path("note/{noteId}/paragraph/{paragraphId}/kill")
  public Response cancelParagraph(@Context HttpServletRequest req,
                                  @PathParam("noteId") String noteId,
                                  @PathParam("paragraphId") String paragraphId) {
    if (!QuboleACLHelper.isOperationAllowed(noteId, req, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    Note note = notebook.getNote(noteId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "Note not found:", noteId).build();
    }
    Paragraph paragraph = note.getParagraph(paragraphId);
    if (paragraph == null) {
      return new JsonResponse(Status.NOT_FOUND, "Paragraph not found:", paragraphId).build();
    }
    try {
      LOG.info("Trying to abort paragraph: " + paragraphId);
      paragraph.abort();
      return new JsonResponse(Status.OK, "Paragraph aborted successfully.", paragraphId).build();
    } catch (Exception ex) {
      LOG.error("Exception while aborting paragraph: " + paragraphId, ex);
      return new JsonResponse(
          Status.INTERNAL_SERVER_ERROR,
          "Exception while aborting paragraph.",
          ex.getMessage()).build();
    }
  }

  /**
   * Associate note with this cluster
   * @throws IOException
   */
  @PUT
  @Path("note/associate/{noteId}")
  public Response associateNote(@Context HttpServletRequest request,
                        @PathParam("noteId") String noteId, String req)
      throws IOException {

    Note note = notebook.getNote(noteId);
    if (note == null) {
      Map<String, String> noteAttributes = QuboleNoteAttributes.getNoteAttributesFromJSON(req);
      note = notebook.fetchAndLoadNoteFromObjectStore(noteId, noteAttributes);
      QuboleNoteAttributes.setNoteAttributes(note, noteAttributes);
      if (note == null) {
        LOG.error("Associate failed for note " + noteId);
        return new JsonResponse<>(Status.NOT_FOUND).build();
      }
      if (!QuboleACLHelper.isOperationAllowed(noteId, request, notebook, WRITE)) {
        notebook.removeNote(noteId, null);
        return new JsonResponse<>(Status.FORBIDDEN).build();
      }

      ZeppelinServer.notebookWsServer.refresh(note);
      LOG.info("Succesfully processed associate request for note " + noteId);
    }
    String userId = request.getHeader(QuboleServerHelper.QBOL_USER_ID);
    QuboleEventUtils.saveEvent(EVENTTYPE.NOTEBOOK_ASSOCIATE, userId, note);
    return new JsonResponse<>(Status.OK).build();
  }

  /**
   * Create new note
   *
   * @throws IOException
   * @throws CloneNotSupportedException
   */
  @POST
  @Path("note")
  public Response createNote(@Context HttpServletRequest request,
      @QueryParam("name") String name,
      @QueryParam("sourceNoteId") String sourceNoteId,
      @QueryParam("source") String source,
      @QueryParam("interpreterIds") List<String> interpreterIds)
      throws IOException {
    //All operations for JobServer are allowed.
    /*
    if (!QuboleACLHelper.canCreateNote(request)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }
    */

    // Create the JSON Object
    JsonObject propObj = new JsonObject();

    propObj.addProperty("id", sourceNoteId);
    propObj.addProperty("name", name);
    propObj.addProperty("source", source);
    JsonArray arr = new JsonArray();
    for (String id : interpreterIds) {
      arr.add(new JsonPrimitive(id));
    }
    propObj.add("interpretersettings", arr);

    JsonObject dataObj = new JsonObject();
    dataObj.add("data", propObj);
    if (!StringUtils.isEmpty(sourceNoteId)) {
      return new JsonResponse(Status.FORBIDDEN, "Cannot clone from native UI").build();
    }
    Note note = (interpreterIds == null || interpreterIds.isEmpty()) ?
        notebook.createNote(null) : notebook.createNote(interpreterIds, null);
    source = (source == null) ? "JobServer" : source;
    if (!StringUtils.isEmpty(name)) {
      note.setName(name);
    }
    Map<String, String> obj = new HashMap<>();
    obj.put("id", note.id());
    return new JsonResponse(Status.CREATED, null, obj).build();
  }
  /**
   *  update note
   * @throws IOException
   *
   */
  @PUT
  @Path("note/{noteId}")
  public Response updateNote(@Context HttpServletRequest request,
                             @PathParam("noteId") String noteId, String req) throws IOException {
    if (!QuboleACLHelper.isOperationAllowed(noteId, request, notebook, WRITE)) {
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }

    // Create the JSON Object
    JsonObject propObj = (JsonObject) new JsonParser().parse(req);
    Note note = notebook.getNote(noteId);
    JsonElement jsonElement = propObj.get("name");
    if (jsonElement != null) {
      String name = jsonElement.getAsString();
      note.setName(name);
      note.persist(null);
      QuboleUtil.updateNoteNameChangeInRails(note);
      ZeppelinServer.notebookWsServer.refresh(note);
    }
    
    jsonElement = propObj.get("location");
    if (jsonElement != null) {
      String newS3Loc = jsonElement.getAsString();
      if (StringUtils.isEmpty(newS3Loc)) {
        LOG.error("New note location for noteId {} is null or empty.", noteId);
        return new JsonResponse<>(Status.INTERNAL_SERVER_ERROR).build();
      }
      QuboleNoteAttributes noteAttr = note.getQuboleNoteAttributes();
      if (noteAttr != null) {
        noteAttr.setLocation(newS3Loc);
        LOG.info("Moving note id {} to location {}.", noteId, newS3Loc);
      }
      else {
        LOG.error("Note attributes for noteId {} not found.", noteId);
        return new JsonResponse<>(Status.INTERNAL_SERVER_ERROR).build();
      }
    }

    JsonResponse<String> jsonResponse = new JsonResponse<String>(Status.OK);
    return jsonResponse.build();
  }

  @PUT
  @Path("qlog")
  public Response receiveAndSendQlog(String req) throws IOException {
    return QuboleServerHelper.receiveAndSendQlog(req, notebook);
  }

  /**
   * This API is used by Qubole-Tapp to notify
   * zeppelin that some permission (roles, groups or
   * users belonging to groups) has changed and zeppelin
   * needs to refresh ACLs from tapp.
   */
  @GET
  @Path("refresh_acl")
  public Response refreshQuboleAcls(String req) throws IOException {
    QuboleACLHelper.refreshACLs(notebook, null);
    return new JsonResponse<>(Status.OK).build();
  }

  /** In case only a notebook related acl change,
   *  this API can be called to refresh only the acls
   *  related to that notebook.
   */
  @GET
  @Path("note/refresh_acl/{noteId}")
  public Response refreshQuboleAclsForNotebook(@PathParam("noteId") String noteId) {
    QuboleACLHelper.refreshACLs(notebook, noteId);
    return new JsonResponse<>(Status.OK).build();
  }

  /**
   * Associate new notes in bulk.
   * @param message - newNoteId to newS3Location map.
   * @return HTTP Status
   * @throws IOException
   */
  @PUT
  @Path("/associate")
  public Response associateNotes(@Context HttpServletRequest req, String message)
      throws IOException {
    Gson gson = new Gson();
    if (!QuboleACLHelper.canCreateNote(req)) {
      LOG.error("Not enough permission to move notes.");
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }
    Notebook notebook = ZeppelinServer.notebook;
    Map<String, Object> noteAttrsMap = gson.fromJson(message, Map.class);
    for (String noteId : noteAttrsMap.keySet()) {
      Note note = notebook.getNote(noteId);
      if (note == null) {
        Map<String, String> noteAttributes =
            QuboleNoteAttributes.getNoteAttributesFromJSON(gson.toJson(noteAttrsMap.get(noteId)));
        note = notebook.fetchAndLoadNoteFromObjectStore(noteId, noteAttributes);
        if (note == null) {
          LOG.error("Associate failed for note " + noteId);
          return new JsonResponse<>(Status.NOT_FOUND).build();
        }
        QuboleNoteAttributes.setNoteAttributes(note, noteAttributes);
        ZeppelinServer.notebookWsServer.refresh(note);
        LOG.info("Succesfully processed associate request for note " + noteId);
      }
      String userId = req.getHeader(QuboleServerHelper.QBOL_USER_ID);
      QuboleEventUtils.saveEvent(EVENTTYPE.NOTEBOOK_ASSOCIATE, userId, note);
    }
    QuboleUtil.syncNotesToFolders();
    return new JsonResponse<>(Status.OK).build();
  }
  
  /**
   * Update location of existing notes.
   * @param message - noteId to newS3Location map.
   * @return HTTP Status
   * @throws IOException
   */
  @POST
  @Path("/update")
  public Response updateNotes(@Context HttpServletRequest req, String message)
      throws IOException {
    Gson gson = new Gson();
    if (!QuboleACLHelper.canCreateNote(req)) {
      LOG.error("Not enough permission to move notes.");
      return new JsonResponse<>(Status.FORBIDDEN).build();
    }
    Notebook notebook = ZeppelinServer.notebook;
    Map<String, String> noteIdToS3LocMap = gson.fromJson(message, Map.class);
    for (String noteId : noteIdToS3LocMap.keySet()) {
      Note note = notebook.getNote(noteId);
      synchronized (note) {
        if (note != null) {
          QuboleNoteAttributes noteAttr = note.getQuboleNoteAttributes();
          if (noteAttr != null) {
            String newS3Loc = noteIdToS3LocMap.get(noteId);
            if (!StringUtils.isEmpty(newS3Loc)) {
              noteAttr.setLocation(newS3Loc);
              LOG.info("Moving note id {} to location {}.", noteId, newS3Loc);
            } else {
              LOG.error("New note location for noteId {} is null or empty.", noteId);
              return new JsonResponse<>(Status.INTERNAL_SERVER_ERROR).build();
            }
          } else {
            LOG.error("Note attributes for noteId {} not found.", noteId);
            return new JsonResponse<>(Status.INTERNAL_SERVER_ERROR).build();
          }
        } else {
          LOG.error("Note with noteId {} not found.", noteId);
          return new JsonResponse<>(Status.INTERNAL_SERVER_ERROR).build();
        }
      }
    }
    QuboleUtil.syncNotesToFolders();
    return new JsonResponse<>(Status.OK).build();
  }
  
  /**
   * Delete notes in bulk
   * @param message - List of noteId's.
   * @return HTTP Status
   * @throws IOException
   */
  @DELETE
  @Path("/delete")
  public Response deleteNotes(@Context HttpServletRequest req, String message)
      throws IOException {
    Gson gson = new Gson();
    Notebook notebook = ZeppelinServer.notebook;
    List<String> notesToDelete = gson.fromJson(message, List.class);
    for (String noteId : notesToDelete) {
      try {
        Note note = notebook.getNote(noteId);
        synchronized (note) {
          if (!QuboleACLHelper.isOperationAllowed(noteId, req, notebook, DELETE)) {
            return new JsonResponse<>(Status.FORBIDDEN).build();
          }
          notebook.removeNote(noteId, null);
          LOG.info("Removing note id {}", noteId);
        }
      } catch (Exception e) {
        LOG.error("Exception occured when deleting note {} : {}", noteId, e.getMessage());
        return new JsonResponse<>(Status.INTERNAL_SERVER_ERROR).build();
      }
    }
    return new JsonResponse<>(Status.OK).build();
  }
}
