package producer_consumer;

import kafka.EmbeddedSingleNodeKafkaCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class BaseClusterIT {
    /**
     * Кафка кластер
     */
    protected static EmbeddedSingleNodeKafkaCluster cluster;
    @BeforeAll
    public static void initCluster() throws Exception {
        cluster = new EmbeddedSingleNodeKafkaCluster();
        //перед всеми тестами запускаем кафку кластер для обработки сообщений
        cluster.start();
        assertTrue(cluster.isRunning());
    }

    @AfterAll
    public static void stopCluster(){
        //закрываем кафку кластер после всех тестов
        cluster.stop();
    }

}
