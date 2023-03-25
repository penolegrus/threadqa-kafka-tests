package docker_tests;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static docker_tests.ManualContainersSetup.JSON_SERVER_EXPOSED;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LateInitContainersTest {
    @Test
    public void manual_init_containers_success_flow_test() {
        //запускаем в докер контейнерах разные сервисы
        ManualContainersSetup.getInstance();
        assertTrue(ManualContainersSetup.kafka.isRunning());
        assertTrue(ManualContainersSetup.schemaRegistry.isRunning());
        assertTrue(ManualContainersSetup.jsonServer.isRunning());
        assertTrue(ManualContainersSetup.httpProducer.isRunning());
        assertTrue(ManualContainersSetup.httpMaterializer.isRunning());

        //отправляем запрос и проверяем что сообщения пришли
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
}
