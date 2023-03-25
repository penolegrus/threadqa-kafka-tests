package producer.domain;


import models.AvroMessage;

public final class MessageTransformer {
    public MessageModel transform(AvroMessage avroMessage) {
        return new MessageModel(avroMessage.getId(), avroMessage.getFrom(), avroMessage.getTo(), avroMessage.getText());
    }

    public AvroMessage transform(MessageModel messageModel) {
        return AvroMessage.newBuilder()
                .setId(messageModel.getId())
                .setFrom(messageModel.getFrom())
                .setTo(messageModel.getTo())
                .setText(messageModel.getText())
                .build();
    }
}
