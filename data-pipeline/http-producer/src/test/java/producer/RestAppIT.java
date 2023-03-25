package producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.dropwizard.testing.DropwizardTestSupport;
import kafka.EmbeddedSingleNodeKafkaCluster;
import models.AvroMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import producer.domain.MessageModel;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RestAppIT {
    private static final String topic = "topic1";
    private static ProducerRestConfig producerRestConfig;
    private static DropwizardTestSupport<ProducerRestConfig> SUPPORT;
    private static EmbeddedSingleNodeKafkaCluster CLUSTER;

    @BeforeAll
    public static void initCluster() throws Exception {
        CLUSTER = new EmbeddedSingleNodeKafkaCluster();
        CLUSTER.start();

        // инициализируем тестовый сервис который обрабатывает независимые запросы
        producerRestConfig =
                new ProducerRestConfig(
                        "test-config-1",
                        new ProducerRestConfig.KafkaClientFactory(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(), topic));
        SUPPORT = new DropwizardTestSupport<>(RestApp.class, producerRestConfig);
        SUPPORT.before();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
        SUPPORT.after();
        producerRestConfig = null;
        SUPPORT = null;
    }

    @Test
    public void sendPostRequest_msgProduced_success() {
        Client client = new JerseyClientBuilder().build();

        Response response =
                client
                        .target(String.format("http://localhost:%d/messages", SUPPORT.getLocalPort()))
                        .request()//создаем модель с полными значениями
                        .post(Entity.json(new MessageModel("id-1", "from-1", "to-1", "text-1")));

        assertEquals(202, response.getStatus());

        final KafkaConsumer<String, AvroMessage> consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singletonList(topic));
        final ArrayList<AvroMessage> messages = new ArrayList<>();

        await()
                .atMost(25, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {//проверяем что сообщение доставлено, так как модель имеет все заполненные поля
                            final ConsumerRecords<String, AvroMessage> records = consumer.poll(Duration.ofMillis(100));
                            records.forEach(record -> messages.add(record.value()));
                            assertEquals(1, messages.size());
                        });
    }

    @Test
    public void sendPostRequest_msgProduced_fail() {
        Client client = new JerseyClientBuilder().build();

        Response response =
                client
                        .target(String.format("http://localhost:%d/messages", SUPPORT.getLocalPort()))
                        .request()
                        // пробуем построить модель с null значениями
                        .post(Entity.json(new MessageModel(null, null, "null", "")));

        // получаем ошибка
        assertEquals(400, response.getStatus());

        final KafkaConsumer<String, AvroMessage> consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singletonList(topic));
        final ArrayList<AvroMessage> messages = new ArrayList<>();

        await()
                .pollDelay(5, TimeUnit.SECONDS)
                .untilAsserted( //проверяем что сообщение не доставлено
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
        properties.put(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "gr-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }
}
