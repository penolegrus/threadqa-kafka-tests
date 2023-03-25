package materializer.infrastructure.service;

import java.util.ArrayList;
import java.util.List;

import materializer.domain.MessageModel;

public class DatabaseWebServiceInMemmory implements DatabaseWebService {
    private List<MessageModel> messages;

    public DatabaseWebServiceInMemmory() {
        this.messages = new ArrayList<>();
    }

    @Override
    public void saveMessage(MessageModel message) {
        messages.add(message);
        System.out.println(message);
    }

    @Override
    public List<MessageModel> getMessages() {
        return messages;
    }
}
