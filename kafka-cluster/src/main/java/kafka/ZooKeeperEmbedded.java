package kafka;

import org.apache.curator.test.TestingServer;

import java.io.IOException;

/**
 * https://github.com/confluentinc/kafka-streams-examples/tree/5.2.0-post/src/test/java/io/confluent/examples/streams/zookeeper
 *
 * Запускает сервер zookeeper в runtime режиме при инициализации класса
 *
 */
public class ZooKeeperEmbedded {

  private final TestingServer server;

  /**
   * Создает экземпляр и запускает ZooKeeper сервер
   *
   * @throws Exception
   */
  public ZooKeeperEmbedded() throws Exception {
    this.server = new TestingServer();
  }

  /**
   * Останавливает сервер
   * @throws IOException
   */
  public void stop() throws IOException {
    server.close();
  }

  /**
   * Строка с подключением к ZooKeeper в конфиге `zookeeper.connect` в формате айпи адреса и сервера.
   * Например 127.0.0.1:2181
   *
   * Можно использовать при подключении кафки к zookeeper
   */
  public String connectString() {
    return server.getConnectString();
  }

  /**
   * Название хоста у экземпляра zookeeper
   * Например 127.0.0.1
   */
  public String hostname() {
    // "server:1:2:3" -> "server:1:2"
    return connectString().substring(0, connectString().lastIndexOf(':'));
  }

}
