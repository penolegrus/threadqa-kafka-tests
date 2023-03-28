package docker_tests;

import io.restassured.http.ContentType;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class EmbeddedContainerEnvTest {
    private String JSON_SERVER_URL;
    @Container
    public static DockerComposeContainer<?> env =
            new DockerComposeContainer(new File("src/test/resources/e2e/docker-compose.test.yml"))
                    .withLocalCompose(true)
                    .withOptions("--compatibility")
                    .waitingFor("db-mock_1", Wait.forHttp("/").forStatusCode(200))
                    .waitingFor("schema-registry_1", Wait.forHttp("/subjects").forStatusCode(200))
                    .waitingFor("http-producer_1", Wait.forHttp("/messages").forStatusCode(200))
                    .withExposedService("db-mock_1", 80)
                    .withExposedService("http-producer_1", 8080);

    private final Supplier<ConditionFactory> WAITER = () -> Awaitility.given()
            .ignoreExceptions()
            .pollInterval(3, TimeUnit.SECONDS)
            .await()
            .dontCatchUncaughtExceptions()
            .atMost(10, TimeUnit.SECONDS);

    /**
     * Ждет пока сообщение придет в систему
     * @param id айди сообщения
     * @return модель сообщения
     */
    private MessageModel waitMessage(String id){
        AtomicReference<MessageModel> messageModel = new AtomicReference<>();
        WAITER.get().until(() -> {
            messageModel.set(given()
                    .baseUri(JSON_SERVER_URL)
                    .pathParam("id", id)
                    .get("/messages/{id}")
                    .then().log().body()
                    .extract()
                    .as(MessageModel.class));
            return messageModel.get().getId().equals(id);
        });
        return messageModel.get();
    }


    @Test
    public void test_data_pipeline_flow_successful() {
        final String HTTP_PRODUCER_BASE_URL = "http://" + env.getServiceHost("http-producer_1", 8080) +
                ":" + env.getServicePort("http-producer_1", 8080);
        JSON_SERVER_URL = "http://" + env.getServiceHost("db-mock_1", 80)
                + ":" + env.getServicePort("db-mock_1", 80);

        String id = UUID.randomUUID().toString();
        String from = UUID.randomUUID().toString();
        String to = UUID.randomUUID().toString();
        String text = UUID.randomUUID().toString();

        MessageModel message = new MessageModel(id, from, to, text);

        given()
                .contentType(ContentType.JSON)
                .baseUri(HTTP_PRODUCER_BASE_URL)
                .body(message)
                .post("/messages")
                .then().log().body()
                .statusCode(202);

        // синхрон
        // MessageModel model = waitMessage(id);

        // асинхрон
        await()
                .pollInterval(3, TimeUnit.SECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            MessageModel messageResponse =
                                    given()
                                            .baseUri(JSON_SERVER_URL)
                                            .get("/messages/" + id)
                                            .then().log().body()
                                            .extract()
                                            .as(MessageModel.class);
                            assertEquals(id, messageResponse.getId());
                        });
    }

}
