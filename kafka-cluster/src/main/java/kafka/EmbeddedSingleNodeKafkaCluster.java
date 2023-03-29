package kafka;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.*;

// https://github.com/confluentinc/kafka-streams-examples/blob/5.2.0-post/src/test/java/io/confluent/examples/streams/kafka/EmbeddedSingleNodeKafkaCluster.java

public class EmbeddedSingleNodeKafkaCluster {
  private static final String KAFKASTORE_OPERATION_TIMEOUT_MS = "60000";
  private static final String KAFKASTORE_INIT_TIMEOUT = "90000";
  private ZooKeeperEmbedded zookeeper;
  private KafkaEmbedded broker;
  private RestApp schemaRegistry;
  private final Properties brokerConfig;
  private boolean running;
  /**
   * Создает и стартует кластер
   */
  public EmbeddedSingleNodeKafkaCluster() {
    this(new Properties());
  }

  /**
   * Создает и стартуер кластер с доп настройками кафки
   *
   * @param brokerConfig настройки кафки
   */
  public EmbeddedSingleNodeKafkaCluster(final Properties brokerConfig) {
    this.brokerConfig = new Properties();
    this.brokerConfig.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, KAFKASTORE_OPERATION_TIMEOUT_MS);
    this.brokerConfig.putAll(brokerConfig);
  }

  /**
   * Init ZooKeeper и стартует Kafka сервер в runtime режиме
   */
  public void start() throws Exception {
    zookeeper = new ZooKeeperEmbedded();
    final Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);

    broker = new KafkaEmbedded(effectiveBrokerConfig);

    final Properties schemaRegistryProps = new Properties();

    schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, KAFKASTORE_OPERATION_TIMEOUT_MS);
    schemaRegistryProps.put(SchemaRegistryConfig.DEBUG_CONFIG, "true");
    schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG, KAFKASTORE_INIT_TIMEOUT);

    schemaRegistry = new RestApp(0, zookeeperConnect(), "_schemas", AvroCompatibilityLevel.NONE.name, schemaRegistryProps);
    schemaRegistry.start();
    running = true;
  }

  private Properties effectiveBrokerConfigFrom(final Properties brokerConfig, final ZooKeeperEmbedded zookeeper) {
    final Properties effectiveConfig = new Properties();
    effectiveConfig.putAll(brokerConfig);
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), 30 * 1000);
    effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), 0);
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), 60 * 1000);
    effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
    effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
    effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
    effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
    return effectiveConfig;
  }

  /**
   * Останавливает все запущенные процессы
   */
  public void stop() {
    try {
      try {
        if (schemaRegistry != null) {
          schemaRegistry.stop();
        }
      } catch (final Exception fatal) {
        throw new RuntimeException(fatal);
      }
      if (broker != null) {
        broker.stop();
      }
      try {
        if (zookeeper != null) {
          zookeeper.stop();
        }
      } catch (final IOException fatal) {
        throw new RuntimeException(fatal);
      }
    } finally {
      running = false;
    }
  }

  /**
   * Бутстрап переменная серверов bootstrap.servers для подключения к кластеру у кафки продюсеров, консьюмеров и стримовых приложений
   * Например 127.0.0.1:9092
   */
  public String bootstrapServers() {
    return broker.brokerList();
  }

  /**
   * Zookeeper переменная zookeeper.connect для подключения к кластеру у кафка консьюмеров
   * Например: 127.0.0.1:2181
   */
  public String zookeeperConnect() {
    return zookeeper.connectString();
  }

  /**
   * Регистр схемы schema.registry.url
   */
  public String schemaRegistryUrl() {
    return schemaRegistry.restConnect;
  }

  /**
   * Создает кафку топик с 1 разделом и репликацией.
   *
   * @param topic Название топика
   */
  public void createTopic(final String topic) {
    createTopic(topic, 1, (short) 1, Collections.emptyMap());
  }

  /**
   * Создает кафка топик с параметрами
   *
   * @param topic       Название топика
   * @param partitions  Количество разделом в топике
   * @param replication Количество репликаций раздела в топике
   */
  public void createTopic(final String topic, final int partitions, final short replication) {
    createTopic(topic, partitions, replication, Collections.emptyMap());
  }

  /**
   * Создает кафка топик с параметрами
   *
   * @param topic       Название топика
   * @param partitions  Количество разделом в топике
   * @param replication Количество репликаций раздела в топике
   * @param topicConfig Дополнительные параметры
   */
  public void createTopic(final String topic, final int partitions, final short replication,
                          final Map<String, String> topicConfig) {
    broker.createTopic(topic, partitions, replication, topicConfig);
  }

  /**
   * Удаляет топики из кафки, блокирует все до тех пор пока топики не будут удалены
   *
   * @param timeoutMs Таймаут милисекунд для удаления топиков
   * @param topics    Названия топиков
   */
  public void deleteTopicsAndWait(final long timeoutMs, final String... topics) throws InterruptedException {
    for (final String topic : topics) {
      try {
        broker.deleteTopic(topic);
      } catch (final UnknownTopicOrPartitionException expected) {
        // показывает идемпотентный результат
      }
    }

    if (timeoutMs > 0) {
      TestUtils.waitForCondition(new TopicsDeletedCondition(topics), timeoutMs, "Topics not deleted after " + timeoutMs + " milli seconds.");
    }
  }

  /**
   * Проверяет запущен ли кафка кластер
   * @return
   */
  public boolean isRunning() {
    return running;
  }

  /**
   * Класс для удаления топика по SOLID принципам
   */
  private final class TopicsDeletedCondition implements TestCondition {
    final Set<String> deletedTopics = new HashSet<>();

    private TopicsDeletedCondition(final String... topics) {
      Collections.addAll(deletedTopics, topics);
    }

    @Override
    public boolean conditionMet() {
      final Set<String> allTopics = new HashSet<>(
          JavaConverters.seqAsJavaListConverter(broker.getKafkaServer().zkClient().getAllTopicsInCluster()).asJava());
      return !allTopics.removeAll(deletedTopics);
    }
  }
}
