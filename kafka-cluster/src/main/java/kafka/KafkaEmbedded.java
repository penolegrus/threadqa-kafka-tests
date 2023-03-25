package kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Запускает локальную Кафку в runtime по дефолтному адресу 127.0.0.1:9092
 * Чтобы все работало, нужно иметь запущенный ZooKeeper чтобы подключаться к нему
 * По дефолту ZooKeeper запускается на 127.0.0.1:2181
 * Можно задать собственный адрес для кипера в конфиге `zookeeper.connect`
 */
public class KafkaEmbedded {
    private static final String DEFAULT_ZK_CONNECT = "127.0.0.1:2181";

    private final Properties effectiveConfig;
    private final File logDir;
    private final Path tmpFolder;
    private final KafkaServer kafka;

    /**
     * https://github.com/confluentinc/kafka-streams-examples/blob/5.2.0-post/src/test/java/io/confluent/examples/streams/kafka/KafkaEmbedded.java
     * Создает и запускает локальный кафка сервер в runtime режиме
     *
     * @param config конфигурация брокера. Нельзя менять конфиги для log.dirs и port
     */
    public KafkaEmbedded(final Properties config) throws IOException {
        //создает папку для данных кафка кластера
        tmpFolder = Files.createDirectories(Paths.get("build/cluster_data"));
        //временная папка куда сохраняются логи и данные
        //если нужно хранить созданные топики и сообщения в них, то закомментировать эту часть
        logDir = Files.createTempDirectory(tmpFolder, "kafka_cluster_logs_").toFile();
        effectiveConfig = effectiveConfigFrom(config);
        final boolean loggingEnabled = true;
        final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);
        kafka = TestUtils.createServer(kafkaConfig, Time.SYSTEM);
    }

    /**
     * Настройки для кафки
     *
     * @param initialConfig
     * @return
     */
    private Properties effectiveConfigFrom(final Properties initialConfig) {
        final Properties effectiveConfig = new Properties();
        effectiveConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
        effectiveConfig.put(KafkaConfig$.MODULE$.HostNameProp(), "127.0.0.1");
        effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), "9092");
        effectiveConfig.put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
        effectiveConfig.put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), 1000000);
        effectiveConfig.put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);

        effectiveConfig.putAll(initialConfig);
        effectiveConfig.setProperty(KafkaConfig$.MODULE$.LogDirProp(), logDir.getAbsolutePath());
        return effectiveConfig;
    }

    /**
     * Метаданные из конфига metadata.broker.list например 127.0.0.1:9092
     * Получает список с брокерами для доп информации продюсера и консьюмера
     */
    public String brokerList() {
        return String.join(":", kafka.config().hostName(), Integer.toString(kafka.boundPort(
                ListenerName.forSecurityProtocol(SecurityProtocol
                        .PLAINTEXT))));
    }


    /**
     * Строка для подключения к ZooKeeper
     */
    public String zookeeperConnect() {
        return effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
    }

    /**
     * Останавливает кафку
     */
    public void stop() {
        kafka.shutdown();
        kafka.awaitShutdown();
    }

    /**
     * Создает кафка топик с 1 разделом и 1 репликацией
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

        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());

        try (final AdminClient adminClient = AdminClient.create(properties)) {
            final NewTopic newTopic = new NewTopic(topic, partitions, replication);
            newTopic.configs(topicConfig);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException fatal) {
            throw new RuntimeException(fatal);
        }

    }

    /**
     * Удаляет кафка топик
     *
     * @param topic название топика
     */
    public void deleteTopic(final String topic) {
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());

        try (final AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        } catch (final ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public KafkaServer getKafkaServer() {
        return kafka;
    }
}
