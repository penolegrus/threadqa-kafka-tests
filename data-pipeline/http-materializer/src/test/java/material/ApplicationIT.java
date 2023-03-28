package material;

import avro.Message;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.github.tomakehurst.wiremock.WireMockServer;
import de.mkammerer.wiremock.WireMockExtension;
import kafka.EmbeddedSingleNodeKafkaCluster;
import materializer.MaterializerApp;
import materializer.MaterializerConfig;
import materializer.domain.MessageTransformer;
import materializer.infrastructure.service.DatabaseWebService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

public class ApplicationIT {
    private static final String JSON = "application/json; charset=utf-8";
    public static EmbeddedSingleNodeKafkaCluster CLUSTER;
    private static WireMockServer wireMockServer;

    @BeforeAll
    public static void initCluster() throws Exception {
        CLUSTER = new EmbeddedSingleNodeKafkaCluster();
        CLUSTER.start();
        wireMockServer = new WireMockServer(wireMockConfig().dynamicPort());
        wireMockServer.start();
    }
    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
        wireMockServer.stop();
    }
    @Test
    public void send_InMemory_3Msg_success() throws ExecutionException, InterruptedException {

        String topic = "topic1";
        CLUSTER.createTopic(topic);

        // конфиг
        final String dbRestServiceUrl = "http://somefakeurl:1234/messages";
        final MaterializerConfig.KafkaConfig kafkaConfig = new MaterializerConfig.KafkaConfig(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(), topic);
        final MaterializerConfig.DatabaseRestServiceConfig dbRestServiceConfig = new MaterializerConfig.DatabaseRestServiceConfig(dbRestServiceUrl);
        final MaterializerConfig testConfig = new MaterializerConfig("test", kafkaConfig, dbRestServiceConfig);
        final MaterializerApp materializerApp = new MaterializerApp(testConfig, new MessageTransformer(), true);

        // отправка 3 записей
        final KafkaProducer<String, Message> messageProducer = TestUtils.getMessageProducer(kafkaConfig);
        for (int i = 1; i < 4; i++) {
            final Message msg = Message.newBuilder()
                    .setId(i + "")
                    .setFrom("from-" + i)
                    .setTo("to-" + i)
                    .setText("text-" + i)
                    .build();
            messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg)).get(); // приходят сообщения не сразу
        }

        // инициализируем сервис с базой данных
        final DatabaseWebService dbWebService = materializerApp.getDbWebService();
        // проверяем что в бд записалось 0 сообщений
        assertEquals(0, dbWebService.getMessages().size());
        // запускаем приложение
        materializerApp.start();

        // проверяем что сообщения пришли спустя определенное время
        await().atMost(15, TimeUnit.SECONDS).until(() -> dbWebService.getMessages().size() == 3);

        // останавливаем приложение
        materializerApp.stop();
    }

    @Test
    public void send_3Msg_success() throws ExecutionException, InterruptedException {
        String topic = "topic2";
        CLUSTER.createTopic(topic);

        // конфиг
        String baseUrl = wireMockServer.baseUrl();
        String dbRestServiceUrl = baseUrl + "/messages";
        MaterializerConfig.KafkaConfig kafkaConfig = new MaterializerConfig.KafkaConfig(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(), topic);
        MaterializerConfig.DatabaseRestServiceConfig dbRestServiceConfig = new MaterializerConfig.DatabaseRestServiceConfig(dbRestServiceUrl);
        MaterializerConfig testConfig = new MaterializerConfig("test", kafkaConfig, dbRestServiceConfig);

        MaterializerApp materializerApp = new MaterializerApp(testConfig, new MessageTransformer(), false);

        // wiremock заглушка, ожидаем что при отправке пост запроса, вернется статус 201
        wireMockServer.stubFor(
                post(urlEqualTo("/messages"))
                        .willReturn(aResponse().withStatus(201).withHeader("Content-Type", JSON)));

        // отправляем 3 сообщения
        final KafkaProducer<String, Message> messageProducer = TestUtils.getMessageProducer(kafkaConfig);
        for (int i = 1; i < 4; i++) {
            final Message msg =
                    Message.newBuilder()
                            .setId(i + "")
                            .setFrom("from-" + i)
                            .setTo("to-" + i)
                            .setText("text-" + i)
                            .build();
            messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg)).get(); // приходят сообщения не сразу
        }

        // стартуем приложение
        materializerApp.start();

        // проверяем что сервер отдает 3 сообщения
        await()
                .atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                // проверяем что отправлено 3 пост запроса
                                wireMockServer.verify(3, postRequestedFor(urlEqualTo("/messages"))));
        //закрываем приложение
        materializerApp.stop();
    }

    @Test
    public void send_1Msg_3times_failAfter_2nd() throws ExecutionException, InterruptedException {
        String topic = "topic3";
        CLUSTER.createTopic(topic);

        // конфиг
        String baseUrl = wireMockServer.baseUrl();
        String dbRestServiceUrl = baseUrl + "/messages";
        MaterializerConfig.KafkaConfig kafkaConfig = new MaterializerConfig.KafkaConfig(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(), topic);
        MaterializerConfig.DatabaseRestServiceConfig dbRestServiceConfig = new MaterializerConfig.DatabaseRestServiceConfig(dbRestServiceUrl);
        MaterializerConfig testConfig = new MaterializerConfig("test", kafkaConfig, dbRestServiceConfig);

        MaterializerApp materializerApp = new MaterializerApp(testConfig, new MessageTransformer(), false);

        // запускаем приложение
        materializerApp.start();
        KafkaStreams.State runningState = materializerApp.getKafkaMessageMaterializer().getState();

        assertTrue(runningState.isRunning());

        // инициализируем продюсера
        KafkaProducer<String, Message> messageProducer = TestUtils.getMessageProducer(kafkaConfig);
        Message msg = Message.newBuilder().setId("id-1").setFrom("from-1").setTo("to-1").setText("text-1").build();

        // wiremock создаем заглушку
        wireMockServer.stubFor(
                post(urlEqualTo("/messages"))
                        .willReturn(aResponse().withStatus(201).withHeader("Content-Type", JSON)));
        // отправляем сообщение
        messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg)).get(); // приходит не сразу

        await()
                .atMost(15, TimeUnit.SECONDS) //ждем пока запрос отправится в заглушку
                .untilAsserted(() -> wireMockServer.verify(1, postRequestedFor(urlEqualTo("/messages"))));

        // заглушка с неправильной отправкой запроса, которая вернет статус 409
        wireMockServer.stubFor(
                post(urlEqualTo("/messages"))
                        .willReturn(
                                aResponse()
                                        // будет ошибка, если запись с айди уже существует
                                        .withStatus(409)
                                        .withHeader("Content-Type", JSON)));

        // отправляем запись второй раз
        messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg)).get();
        // отправляем запись третий раз
        messageProducer.send(new ProducerRecord<>(topic, msg.getId(), msg)).get();

        await()
                .atMost(15, TimeUnit.SECONDS)
                // игнорим ошибки, чтобы тест не завалился
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            System.out.println(
                                    "STATE: "
                                            + materializerApp
                                            .getKafkaMessageMaterializer()
                                            .getState()
                                            .isRunning());
                            // проверяем что стрим не запустился из за ошибки 409
                            assertFalse(materializerApp.getKafkaMessageMaterializer().getState().isRunning());
                        });

        // Отправлено 3 сообщения, но стрим завалился и остановился после обработки 2-го сообщения -> только 2 запроса
        // проверяем что отправлено всего 2 сообщение, а третье не обработалось
        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/messages")));

        // закрываем приложение
        materializerApp.stop();
    }
}
