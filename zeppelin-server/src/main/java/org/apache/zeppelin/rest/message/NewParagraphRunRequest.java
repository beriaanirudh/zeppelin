package org.apache.zeppelin.rest.message;

import java.util.HashMap;
import java.util.Map;

/**
 * Create and Run New Paragraph
 *
 */
public class NewParagraphRunRequest {
  String paragraph;
  String title = "";
  Map<String, Object> params = new HashMap<>();
  Map<String, Object> config = new HashMap<>();

  public NewParagraphRunRequest() {

  }

  public String getParagraph() {
    return paragraph;
  }

  public String getTitle() {
    return title;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public Map<String, Object> getConfig() {
    return config;
  }
}
