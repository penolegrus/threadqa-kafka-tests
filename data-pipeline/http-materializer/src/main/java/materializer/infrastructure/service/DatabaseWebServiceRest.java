package materializer.infrastructure.service;

import java.io.IOException;
import java.util.List;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import materializer.MaterializerConfig;
import materializer.domain.MessageModel;

import static io.restassured.RestAssured.given;

public class DatabaseWebServiceRest implements DatabaseWebService {
    private final String url;

    public DatabaseWebServiceRest(final MaterializerConfig applicationConfig) {
        this.url = applicationConfig.databaseRestServiceConfig.url;
    }

    @Override
    public void saveMessage(final MessageModel message) {
        Response response = given().contentType(ContentType.JSON)
                .header("Accept", "application/json; charset=utf-8")
                .body(message).post(url).then().log().body().extract().response();
        if (response.statusCode() != 201) {
            throw new RuntimeException("Request failed with info " + "\n" + response.asPrettyString());
        }
    }

    // ignored
    @Override
    public List<MessageModel> getMessages() {
        return null;
    }
}
