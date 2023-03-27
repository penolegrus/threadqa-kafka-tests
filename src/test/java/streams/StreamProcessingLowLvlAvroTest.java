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
import topology.StreamProcessingLowLvlAvro;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StreamProcessingLowLvlAvroTest {
    private final String topicIn = "topic-in";
    private final String topicOut = "topic-out";
    private TopologyTestDriver testDriver;
    private MockSchemaRegistryClient schemaRegistryClient;
    private Properties properties;

    @BeforeEach
    public void start() {
        properties = new Properties();
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "client-id-test-1");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-id-test-5");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "someurl:9922");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://keks:4242");
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
    public void testTopologyLowLvlAvro_statefulProcessors() {
        final String storeName = "same-name";
        // Serde - объект для сериализации и десериализации сообщения
        // создаем серду с конифогом чтобы можно было подключаться к регистру мок схемы
        final SpecificAvroSerde<Person> serde = new SpecificAvroSerde<>(schemaRegistryClient);

        final Map<String, String> schema =
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        "some-fake-url-mocked-threadqa");
        serde.configure(schema, false);
        // получаем топологию
        final Topology topology = StreamProcessingLowLvlAvro.topologyDedupByUserId(topicIn, topicOut, serde, storeName);
        testDriver = new TopologyTestDriver(topology, properties);

        final ConsumerRecordFactory<String, Person> factory = new ConsumerRecordFactory<>(topicIn, new StringSerializer(), serde.serializer());

        Person person1 = Person.newBuilder().setId("id-1").setName("Oleg").setLastname("Threadqa").build();
        final ConsumerRecord<byte[], byte[]> inRecord1 = factory.create(topicIn, "id-1", person1);

        //пускаем данные в пайплайн
        testDriver.pipeInput(Collections.singletonList(inRecord1));
        final KeyValueStore<String, Person> dedupStore = testDriver.getKeyValueStore(storeName);
        final Person person = dedupStore.get("id-1");
        final long l = dedupStore.approximateNumEntries();
        System.out.println("Here : " + l);

        // проверяем что все ок
        assertEquals("id-1", person.getId());
    }

    @Test
    public void testTopologyLowLvlAvro_statefulProcessors_invalidInput() {
        final String storeName = "same-name";

        // создаем серду с конифогом чтобы можно было подключаться к регистру мок схемы
        final SpecificAvroSerde<Person> serde = new SpecificAvroSerde<>(schemaRegistryClient);

        final Map<String, String> schema =
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        "some-fake-url-mocked-threadqa");
        serde.configure(schema, false);
        //получаем топологию
        final Topology topology =
                StreamProcessingLowLvlAvro.topologyDedupByUserId(topicIn, topicOut, serde, storeName);
        testDriver = new TopologyTestDriver(topology, properties);

        final ConsumerRecordFactory<String, Person> factory =
                new ConsumerRecordFactory<>(topicIn, new StringSerializer(), serde.serializer());

        Person person1 = Person.newBuilder().setId("id-1").setName("Naruto").setLastname("Uzumaki").build();
        final ConsumerRecord<byte[], byte[]> inRecord1 = factory.create(topicIn, "invalid-id", person1);

        Person person2 = Person.newBuilder().setId("id-1").setName("Itachi").setLastname("Uchiha").build();
        final ConsumerRecord<byte[], byte[]> inRecord2 = factory.create(topicIn, "id-1", person2);

        Person person3 = Person.newBuilder().setId("id-1").setName("42").setLastname("42").build();
        final ConsumerRecord<byte[], byte[]> inRecord3 = factory.create(topicIn, "id-1", person3);


        testDriver.pipeInput(Arrays.asList(inRecord1, inRecord2, inRecord3));
        final KeyValueStore<String, Person> dedupStore = testDriver.getKeyValueStore(storeName);
        final Person person = dedupStore.get("id-1");

        final ProducerRecord<String, Person> outRecord1 =
                testDriver.readOutput(topicOut, new StringDeserializer(), serde.deserializer());
        final ProducerRecord<String, Person> outRecord2 =
                testDriver.readOutput(topicOut, new StringDeserializer(), serde.deserializer());
        final ProducerRecord<String, Person> outRecord3 =
                testDriver.readOutput(topicOut, new StringDeserializer(), serde.deserializer());

        assertEquals("Itachi", outRecord1.value().getName());
        assertEquals("id-1", outRecord1.key());
        assertEquals("id-1", person.getId());
        assertNull(outRecord2);
        assertNull(outRecord3);

        assertEquals("Uchiha", dedupStore.get("id-1").getLastname());
        assertNull(dedupStore.get("invalid-id"));

        assertEquals(1, dedupStore.approximateNumEntries());
    }
}
