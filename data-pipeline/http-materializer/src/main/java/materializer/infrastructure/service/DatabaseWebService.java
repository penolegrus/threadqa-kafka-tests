package materializer.infrastructure.service;

import java.util.List;

import materializer.domain.MessageModel;

public interface DatabaseWebService {
    void saveMessage(MessageModel message);

    List<MessageModel> getMessages();
}
