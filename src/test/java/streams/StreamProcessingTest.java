package streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
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
import topology.StreamProcessing;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StreamProcessingTest {
  private final String topicIn = "topic-in";
  private final String topicOut = "topic-out";

  /**
   * Удобный фреймворк для тестирования приложений в Kafka Streams
   */
  private TopologyTestDriver testDriver;
  private Properties properties;

  @BeforeEach
  public void start() {
    properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
  }

  @AfterEach
  public void tearDown() {
    Optional.ofNullable(testDriver).ifPresent(TopologyTestDriver::close);
    testDriver = null;
    properties = null;
  }

  @Test
  public void testTopology_statelessProcessors_Uppercase() {
    // создаем топологию, которая изменяет значения в реальном времени (меняет нижний регистр на верхний)
    Topology topology = StreamProcessing.topologyUpperCase(topicIn, topicOut);
    testDriver = new TopologyTestDriver(topology, properties);
    ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(topicIn, new StringSerializer(), new StringSerializer());
    ConsumerRecord<byte[], byte[]> record1 = factory.create(topicIn, null, "magic");
    ConsumerRecord<byte[], byte[]> record2 = factory.create(topicIn, null, "threadqa");

    // пускаем данные в поток
    testDriver.pipeInput(Arrays.asList(record1, record2));
    ProducerRecord<String, String> outRecord1 = testDriver.readOutput(topicOut, new StringDeserializer(), new StringDeserializer());
    ProducerRecord<String, String> outRecord2 = testDriver.readOutput(topicOut, new StringDeserializer(), new StringDeserializer());
    ProducerRecord<String, String> outRecord3 = testDriver.readOutput(topicOut, new StringDeserializer(), new StringDeserializer());

    // проверяем потоковые изменения
    assertNull(outRecord3);
    assertEquals("magic".toUpperCase(), outRecord1.value());
    assertEquals("threadqa".toUpperCase(), outRecord2.value());
  }

  @Test
  public void testTopology_statefullProcessors_Anagram() {
    // создаем топологию, которая взаимодействует с данными в реальном времени (считает количество анаграм)
    String storeName = "count-storage";
    Topology topology = StreamProcessing.topologyCountAnagram(topicIn, topicOut, storeName);
    testDriver = new TopologyTestDriver(topology, properties);
    ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(topicIn, new StringSerializer(), new StringSerializer());
    ConsumerRecord<byte[], byte[]> record1 = factory.create(topicIn, null, "magic");
    ConsumerRecord<byte[], byte[]> record2 = factory.create(topicIn, null, "cigma");

    // пускаем данные в поток
    testDriver.pipeInput(Arrays.asList(record1, record2));
    ProducerRecord<String, Long> outRecord1 = testDriver.readOutput(topicOut, new StringDeserializer(), new LongDeserializer());
    ProducerRecord<String, Long> outRecord2 = testDriver.readOutput(topicOut, new StringDeserializer(), new LongDeserializer());
    ProducerRecord<String, Long> outRecord3 = testDriver.readOutput(topicOut, new StringDeserializer(), new LongDeserializer());

    // получаем значения из хранилища
    final KeyValueStore<String, Long> keyValueStore = testDriver.getKeyValueStore(storeName);
    final Long amountOfRecordsInStorageByKey = keyValueStore.get("acgim");

    // проверки
    assertNull(outRecord3); //третья запись не получена, так как она не отправлена
    assertEquals("acgim", outRecord1.key()); // acgim является анограммой оригинала cigma для 1 записи
    assertEquals("acgim", outRecord2.key()); //acgim является анограммой оригинала magic для 2 записи
    assertEquals(Long.valueOf(1), outRecord1.value()); // количество значений в потоке после первой отправки
    assertEquals(Long.valueOf(2), outRecord2.value()); // количество в потоке после второй отправки, так как значения выходные одинаковые
    assertEquals(Long.valueOf(2), amountOfRecordsInStorageByKey); // количество всех записей по результату топологии
  }
}
