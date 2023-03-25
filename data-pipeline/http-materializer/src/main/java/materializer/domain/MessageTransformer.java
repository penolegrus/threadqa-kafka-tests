package materializer.domain;


import avro.Message;

public final class MessageTransformer {
  public MessageModel transform(final Message message) {
    return new MessageModel(message.getId(), message.getFrom(), message.getTo(), message.getText());
  }

  public Message transform(final MessageModel messageModel) {
    return Message.newBuilder()
        .setId(messageModel.getId())
        .setFrom(messageModel.getFrom())
        .setTo(messageModel.getTo())
        .setText(messageModel.getText())
        .build();
  }
}
