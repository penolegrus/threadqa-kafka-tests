package producer_consumer;

import avroModels.ThreadQaSubscriber;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AvroProducerConsumerIT extends BaseClusterIT {
    @Test
    @SneakyThrows
    // Тест на проверку отправки сообщения продюсером
    public void testAvroProducer() {
        String topic = "topic1";
        cluster.createTopic(topic);

        //строим модель из avro схемы
       ThreadQaSubscriber eventAsync = ThreadQaSubscriber.newBuilder()
                .setId("id-async")
                .setAge(23)
                .setIsQaAutomation(true)
                .setName("Oleg").build();

        // создаем продюсера (брокер сообщений)
        final KafkaProducer<String, ThreadQaSubscriber> producer = new KafkaProducer<>(getProducerProperties());

        // асинхронный способ отправки событий через продюсера без сохранения результата переменной через callback
        producer.send(new ProducerRecord<>(topic, "id-async", eventAsync), ((metadata, exception) -> {
            if (exception == null) {
                System.out.println("Record added");
            } else {
                Assertions.fail();
            }
        }));

        // синхронный способ отправки событий и взаимодействия с ним
        // строим модель из avro схемы
        ThreadQaSubscriber eventSync = ThreadQaSubscriber.newBuilder()
                .setId("id-async")
                .setAge(25)
                .setIsQaAutomation(false)
                .setName("Stas").build();

        // отправляем продюсером сообщение и получаем его через таймаут
        final RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic, "id-sync", eventSync))
                .get(3, TimeUnit.SECONDS);

        // взаимодействие с полями, проверка что сообщение отправлено и оно не Null
        assertTrue(recordMetadata.hasOffset());
        assertTrue(recordMetadata.hasTimestamp());
    }

    @Test
    @SneakyThrows
    //Тест на проверку, что продюсер отправил сообщение в топик и консьюмер его получил
    public void testAvroConsumer() {
        String topic = "threadqa2";
        cluster.createTopic(topic); //создаем топик

        //создаем модель из avro схемы
        ThreadQaSubscriber avroEvent = ThreadQaSubscriber.newBuilder()
                .setId("id-avro-event")
                .setAge(21)
                .setIsQaAutomation(true)
                .setName("JavaLover").build();

        //создаем продюсера для отправки сообщений
        final KafkaProducer<String, ThreadQaSubscriber> producer = new KafkaProducer<>(getProducerProperties());

        //отправляем сообщение
        producer.send(new ProducerRecord<>(topic, "id-avro-event", avroEvent)).get(1, TimeUnit.SECONDS);

        //создаем консьюмера, который будет получать сообщения
        final KafkaConsumer<String, ThreadQaSubscriber> consumer = new KafkaConsumer<>(getConsumerProperties());
        //подписываемся на топики
        consumer.subscribe(Collections.singletonList(topic));

        final ArrayList<ThreadQaSubscriber> events = new ArrayList<>();

        //асинхронный способ проверки условия на полученное сообщение
        await()
                .atMost(15, TimeUnit.SECONDS) //таймаут 15 секунд на асерт условия
                .untilAsserted( // ждем пока условие не будет выполнено (можно любое делать условие)
                        () -> {
                            //каждую секунду получаем новые сообщения из топика на который подписались
                            final ConsumerRecords<String, ThreadQaSubscriber> records = consumer.poll(Duration.ofSeconds(1));
                            records.forEach(record -> {
                                //циклом можно перебрать каждое сообщение и найти нужное в зависимости от ситуации
                                System.out.println(record); //выводим в консоль полученное сообщение
                                events.add(record.value()); //добавляем сообщение в список для ассерта теста
                            });
                            assertEquals(1, events.size()); //проверяем что сообщение пришло
                        });
    }

    private Properties getProducerProperties() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return properties;
    }

    private Properties getConsumerProperties() {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "client0");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.schemaRegistryUrl());
        return properties;
    }
}
