package materializer.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageModel {
  private String id;
  private String from;
  private String to;
  private String text;

  public String json() {
    return "{"
        + "\"id\":"
        + "\""
        + id
        + "\","
        + "\"from\":"
        + "\""
        + from
        + "\","
        + "\"to\":"
        + "\""
        + to
        + "\","
        + "\"text\":"
        + "\""
        + text
        + "\""
        + "}";
  }
}
