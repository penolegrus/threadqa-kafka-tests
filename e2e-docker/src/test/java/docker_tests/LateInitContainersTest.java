package docker_tests;

import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static docker_tests.ManualContainersSetup.HTTP_PRODUCER_EXPOSED;
import static docker_tests.ManualContainersSetup.JSON_SERVER_EXPOSED;
import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LateInitContainersTest {

    @BeforeAll
    public static void initContainers() {
        // запускаем в докер контейнерах разные сервисы
        ManualContainersSetup.getInstance();

        // проверяем что все сервисы запущены
        assertTrue(ManualContainersSetup.kafka.isRunning());
        assertTrue(ManualContainersSetup.schemaRegistry.isRunning());
        assertTrue(ManualContainersSetup.jsonServer.isRunning());
        assertTrue(ManualContainersSetup.httpProducer.isRunning());
        assertTrue(ManualContainersSetup.httpMaterializer.isRunning());
    }

    @Test
    public void manual_init_containers_success_flow_test() {
        // проверяем что сообщения отображаются
        List<MessageModel> messageJsonRepresentations =
                Arrays.asList(
                        given()
                                .baseUri(JSON_SERVER_EXPOSED)
                                .get("/messages")
                                .then().log().all()
                                .statusCode(200)
                                .extract()
                                .as(MessageModel[].class));
        assertTrue(messageJsonRepresentations.size() > 0);
    }

    @Test
    public void add_message_success_test() {
        // генерируем сообщение
        String id = UUID.randomUUID().toString();
        String from = UUID.randomUUID().toString();
        String to = UUID.randomUUID().toString();
        String text = UUID.randomUUID().toString();

        MessageModel message = new MessageModel(id, from, to, text);

        // отправляем сообщение
        given()
                .contentType(ContentType.JSON)
                .baseUri(HTTP_PRODUCER_EXPOSED)
                .body(message)
                .post("/messages")
                .then().log().body()
                .statusCode(202);

        await()
                .pollInterval(3, TimeUnit.SECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            MessageModel messageResponse =
                                    given()
                                            .baseUri(JSON_SERVER_EXPOSED)
                                            .get("/messages/" + id)
                                            .then().log().body()
                                            .extract()
                                            .as(MessageModel.class);
                            assertEquals(id, messageResponse.getId());
                        });

    }
}
