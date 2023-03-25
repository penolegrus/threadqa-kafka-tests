package docker_tests;

import org.testcontainers.containers.*;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.fail;

public final class ManualContainersSetup {
    static final String TOPIC = "events-message-v1";
    static final String CONFLUENT_PLATFORM_VERSION = "5.1.1";

    // окружение кафки
    static KafkaContainer kafka;
    static String KAFKA_BROKER_INSIDE_DOCKER_ENV;
    static GenericContainer<?> schemaRegistry;
    static String SCHEMA_REGISTRY_INSIDE_DOCKER_ENV;

    // окружение приложения
    static GenericContainer<?> jsonServer;
    static String JSON_SERVER_INSIDE_DOCKER_ENV;
    static String JSON_SERVER_EXPOSED;

    static GenericContainer<?> httpProducer;
    static String HTTP_PRODUCER_EXPOSED;
    static GenericContainer<?> httpMaterializer;

    private static ManualContainersSetup INSTANCE;

    public static ManualContainersSetup getInstance() {
        if(INSTANCE == null) {
            INSTANCE = new ManualContainersSetup();
        }
        return INSTANCE;
    }

    private ManualContainersSetup(){
        Network commonNetwork = Network.newNetwork();
        setZookeeperAndKafka(commonNetwork);
        setSchemaRegistry(commonNetwork);
        setJsonServer(commonNetwork);
        setHttProducer(commonNetwork);
        setHttpMaterializer(commonNetwork);
    }

    private static void setZookeeperAndKafka(Network network) {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.1.1"))
                .withNetwork(network);
        kafka.start();
    }

    private static void setSchemaRegistry(Network network) {
        // получает сетевой регистр чтобы остальные контейнеры могли подключаться к окружению
        KAFKA_BROKER_INSIDE_DOCKER_ENV = "PLAINTEXT://" + kafka.getNetworkAliases().get(0) + ":9092";
        schemaRegistry =
                new GenericContainer("confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION)
                        .withExposedPorts(8081)
                        .withNetwork(network)
                        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost") // двустороннее подключение к контейнеру
                        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081") // двустороннее подключение к контейнеру
                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", KAFKA_BROKER_INSIDE_DOCKER_ENV)
                        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
        schemaRegistry.start();
        SCHEMA_REGISTRY_INSIDE_DOCKER_ENV = "http://" + schemaRegistry.getNetworkAliases().get(0) + ":8081";
    }

    private static void setJsonServer(Network network) {
        jsonServer =
                new GenericContainer("zhenik/json-server")
                        .withExposedPorts(80)
                        // all containers put in same network
                        .withNetwork(network)
                        .withEnv("ID_MAP", "id")
                        .withNetworkAliases("json-server")
                        .withClasspathResourceMapping(
                                "e2e/json-server-database.json", "/data/db.json", BindMode.READ_WRITE)
                        .waitingFor(Wait.forHttp("/").forStatusCode(200));

        jsonServer.start();
        // provide availability make http calls from localhost against docker env
        JSON_SERVER_EXPOSED = "http://" + jsonServer.getHost() + ":" + jsonServer.getMappedPort(80);
        JSON_SERVER_INSIDE_DOCKER_ENV = "http://" + jsonServer.getNetworkAliases().get(0) + ":80";
    }

    private static void setHttProducer(Network network) {
        httpProducer =
                new GenericContainer("zhenik/http-producer:data-pipeline")
                        .withExposedPorts(8080)
                        .withNetwork(network)
                        .withEnv("APPLICATION_PORT", "8080")
                        .withEnv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BROKER_INSIDE_DOCKER_ENV)
                        .withEnv("SCHEMA_REGISTRY_URL", SCHEMA_REGISTRY_INSIDE_DOCKER_ENV)
                        .withEnv("SINK_TOPIC", TOPIC)
                        .waitingFor(Wait.forHttp("/messages").forStatusCode(200));
        httpProducer.start();

        HTTP_PRODUCER_EXPOSED = "http://" + httpProducer.getHost() + ":" + httpProducer.getMappedPort(8080);
//        HTTP_PRODUCER_EXPOSED = LOCAL_HOST + ":" + httpProducer.getMappedPort(8080);
    }

    private static void setHttpMaterializer(Network network) {
        createTopic(TOPIC);
        String messageEndpoint = "http://json-server/messages";
        httpMaterializer =
                new GenericContainer("zhenik/http-materializer:data-pipeline")
                        .withNetwork(network)
                        .withEnv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BROKER_INSIDE_DOCKER_ENV)
                        .withEnv("SCHEMA_REGISTRY_URL", SCHEMA_REGISTRY_INSIDE_DOCKER_ENV)
                        .withEnv("DATABASE_REST_SERVICE_URL", messageEndpoint)
                        .withEnv("SOURCE_TOPIC", TOPIC);
        httpMaterializer.start();
    }

    private static void createTopic(String topicName) {
        // кафка контейнер использует запущенный zookeeper
        // confluent platform and Kafka compatibility 5.1.x <-> kafka 2.1.x
        // кафка 2.1.x использует опцию --zookeeper, для более поздних версию надо юзать опцию --bootstrap-servers
        String createTopic =
                String.format(
                        "/usr/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic %s",
                        topicName);
        try {
            final Container.ExecResult execResult = kafka.execInContainer("/bin/sh", "-c", createTopic);
            System.out.println(execResult.toString());
            if (execResult.getExitCode() != 0) {
                fail();
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}
