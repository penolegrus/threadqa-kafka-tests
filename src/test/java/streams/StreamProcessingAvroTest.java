package streams;

import avroModels.Person;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import topology.StreamProcessingAvro;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StreamProcessingAvroTest {
    private final String topicIn = "topic-in";
    private final String topicOut = "topic-out";
    private final String schemaUrl = "http://localhost:8081";
    // http://localhost:8081/subjects/topic-in-value/versions/latest
    // only for TopicNameStrategy
    private final String mockedUrl = schemaUrl + "/subjects/" + topicIn + "-value/versions/latest";
    private TopologyTestDriver testDriver;
    private MockSchemaRegistryClient schemaRegistryClient;
    private Properties properties;

    @BeforeEach
    public void start() {
        properties = new Properties();
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "client-id-test-1");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-id-test-5");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9922");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        schemaRegistryClient = new MockSchemaRegistryClient();
    }

    @AfterEach
    public void tearDown() {
        Optional.ofNullable(testDriver).ifPresent(TopologyTestDriver::close);
        testDriver = null;
        properties = null;
    }

    @Test
    public void testTopologyAvro_statelessProcessors() {
        // регистрируем схему в регистре (не обязательно)
        // schemaRegistryClient.register(TopicNameStrategy().subjectName(topicIn, false, Person.SCHEMA$), Person.SCHEMA$);
        // создаем серду с конфигом чтобы подключаться замоканной регистру схем
        final SpecificAvroSerde<Person> serde = new SpecificAvroSerde<>(schemaRegistryClient);

        final Map<String, String> schema =
                Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "some-fake-url");
        serde.configure(schema, false);
        // получаем топологию
        final Topology topology = StreamProcessingAvro.topologyUpperCase(topicIn, topicOut, serde);
        testDriver = new TopologyTestDriver(topology, properties);

        final ConsumerRecordFactory<String, Person> factory = new ConsumerRecordFactory<>(topicIn, new StringSerializer(), serde.serializer());

        Person person1 = Person.newBuilder().setId("id-1").setName("oleg").setLastname("threadqa").build();
        final ConsumerRecord<byte[], byte[]> inRecord1 = factory.create(topicIn, "1", person1);

        Person person2 = Person.newBuilder().setId("id-2").setName("ivan").setLastname("ivanov").build();
        final ConsumerRecord<byte[], byte[]> inRecord2 = factory.create(topicIn, "2", person2);

        // отправляем сообщения
        testDriver.pipeInput(Arrays.asList(inRecord1, inRecord2));
        final ProducerRecord<String, Person> outRecord1 =
                testDriver.readOutput(topicOut, new StringDeserializer(), serde.deserializer());
        final ProducerRecord<String, Person> outRecord2 =
                testDriver.readOutput(topicOut, new StringDeserializer(), serde.deserializer());

        // ассертим
        assertEquals("ID-1", outRecord1.value().getId());
        assertEquals("ID-2", outRecord2.value().getId());
        assertEquals("ivanov".toUpperCase(), outRecord2.value().getLastname());
    }

    @Test
    public void testTopologyAvro_statefulProcessors() {
        /** Arrange */
        final String storeName = "same-name";
        // регистрируем схему в регистре (не обязательно)
        // schemaRegistryClient.register(new TopicNameStrategy().subjectName(topicIn, false, Person.SCHEMA$), Person.SCHEMA$);

        // создаем серду с конфигом чтобы подключаться замоканной регистру схем
        final SpecificAvroSerde<Person> serde = new SpecificAvroSerde<>(schemaRegistryClient);

        final Map<String, String> schema =
                Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "some-fake-url");
        serde.configure(schema, false);
        // получаем топологию
        final Topology topology =
                StreamProcessingAvro.topologyCountUsersWithSameName(topicIn, topicOut, serde, storeName);
        testDriver = new TopologyTestDriver(topology, properties);

        final ConsumerRecordFactory<String, Person> factory =
                new ConsumerRecordFactory<>(topicIn, new StringSerializer(), serde.serializer());

        Person person1 = Person.newBuilder().setId("id-1").setName("oleg").setLastname("threadqa").build();
        final ConsumerRecord<byte[], byte[]> inRecord1 = factory.create(topicIn, "1", person1);

        Person person2 = Person.newBuilder().setId("id-2").setName("oleg").setLastname("olegov").build();
        final ConsumerRecord<byte[], byte[]> inRecord2 = factory.create(topicIn, "2", person2);

        // пускаем сообщения
        testDriver.pipeInput(Arrays.asList(inRecord1, inRecord2));
        final KeyValueStore<String, Long> keyValueStore = testDriver.getKeyValueStore(storeName);
        final Long amountOfRecordWithSameName = keyValueStore.get("oleg");

        // асертим
        assertEquals(Long.valueOf(2), amountOfRecordWithSameName);
    }
}
