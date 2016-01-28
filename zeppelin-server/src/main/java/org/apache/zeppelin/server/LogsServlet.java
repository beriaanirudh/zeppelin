package org.apache.zeppelin.server;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Servlet class to get the logs
 *
 */
public class LogsServlet extends DefaultServlet {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LogsServlet.class);

  @Override
  protected void sendDirectory(HttpServletRequest request, HttpServletResponse response,
      Resource resource, String pathInContext) throws IOException {
    LOG.info("request to fetch logs");
    byte[] data = null;
    Resource _resourceBase = getResource(pathInContext);
    if (_resourceBase != null) {
      if (_resourceBase instanceof ResourceCollection)
        resource = _resourceBase.addPath(pathInContext);
    }
    // send the base as empty string, as is done for assests
    String dir = resource.getListHTML("", pathInContext.length() > 1);
    if (dir == null) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, "No directory");
      return;
    }
    data = dir.getBytes("UTF-8");
    response.setContentType("text/html; charset=UTF-8");
    response.setContentLength(data.length);
    response.getOutputStream().write(data);
  }
}
