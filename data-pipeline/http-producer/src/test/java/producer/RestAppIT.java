package producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.dropwizard.testing.DropwizardTestSupport;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import kafka.EmbeddedSingleNodeKafkaCluster;
import models.AvroMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import producer.domain.MessageModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RestAppIT {
    private static final String topic = "topic1";
    private static ProducerRestConfig producerRestConfig;

    // https://www.dropwizard.io/en/latest/manual/testing.html гайд по дропвизарду
    private static DropwizardTestSupport<ProducerRestConfig> MOCKED_APP;
    private static EmbeddedSingleNodeKafkaCluster CLUSTER;

    @BeforeAll
    public static void initCluster() throws Exception {
        CLUSTER = new EmbeddedSingleNodeKafkaCluster();
        CLUSTER.start();

        // конфиг с кафкой
        producerRestConfig =
                new ProducerRestConfig("test-config-1",
                        new ProducerRestConfig.KafkaClientFactory(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(), topic));
        // запускаем замоканное приложение, которое будет отправлять запросы в запущенный кафка кластер
        MOCKED_APP = new DropwizardTestSupport<>(RestApp.class, producerRestConfig);
        MOCKED_APP.before();
        RestAssured.baseURI = String.format("http://localhost:%d", MOCKED_APP.getLocalPort());
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
        MOCKED_APP.after();
        producerRestConfig = null;
        MOCKED_APP = null;
    }

    @Test
    public void sendPostRequest_msgProduced_success() {
        MessageModel message = new MessageModel("id-1", "from-1", "to-1", "text-1");

        given().contentType(ContentType.JSON)
                .body(message)
                .post("/messages")
                .then().assertThat().statusCode(202);

        final KafkaConsumer<String, AvroMessage> consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singletonList(topic));
        final ArrayList<AvroMessage> messages = new ArrayList<>();

        await()
                .atMost(25, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            //проверяем что сообщение доставлено, так как модель имеет все заполненные поля
                            final ConsumerRecords<String, AvroMessage> records = consumer.poll(Duration.ofMillis(100));
                            records.forEach(record -> messages.add(record.value()));
                            assertEquals(1, messages.size());
                        });
    }

    @Test
    public void sendPostRequest_msgProduced_fail() {
        // пробуем построить модель с null значениями
        MessageModel model = new MessageModel(null, null, "null", "");

        given().contentType(ContentType.JSON)
                .body(model)
                .post("/messages")
                .then().assertThat().statusCode(400); //проверяем что пришла ошибка из за null в схеме

        final KafkaConsumer<String, AvroMessage> consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singletonList(topic));
        final ArrayList<AvroMessage> messages = new ArrayList<>();

        await()
                .pollDelay(5, TimeUnit.SECONDS)
                .untilAsserted(
                        //проверяем что сообщение не доставлено
                        () -> {
                            final ConsumerRecords<String, AvroMessage> records = consumer.poll(Duration.ofSeconds(1));
                            for (final ConsumerRecord<String, AvroMessage> record : records) {
                                messages.add(record.value());
                            }
                            assertEquals(0, messages.size());
                        });
    }

    private Properties getConsumerProperties() {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "gr-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
