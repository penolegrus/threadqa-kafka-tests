package materializer;

import com.typesafe.config.Config;

public class MaterializerConfig {
    public final String name;
    public final KafkaConfig kafkaConfig;
    public final DatabaseRestServiceConfig databaseRestServiceConfig;

    // дефолтный конструктор
    public MaterializerConfig(String name, KafkaConfig kafkaConfig, DatabaseRestServiceConfig databaseRestServiceConfig) {
        this.name = name;
        this.kafkaConfig = kafkaConfig;
        this.databaseRestServiceConfig = databaseRestServiceConfig;
    }

    public static MaterializerConfig loadConfig(Config config) {
        final String name = config.getString("materializer.name");

        final String bootstrapServers = config.getString("materializer.kafka.bootstrap-servers");
        final String schemaRegistryUrl = config.getString("materializer.kafka.schema-registry-url");
        final String sourceTopic = config.getString("materializer.kafka.source-topic");

        final String dbServiceUrl = config.getString("materializer.database-rest-service.url");

        final MaterializerConfig materializerConfig = new MaterializerConfig(name,
                        new KafkaConfig(bootstrapServers, schemaRegistryUrl, sourceTopic),
                        new DatabaseRestServiceConfig(dbServiceUrl));

        return materializerConfig;
    }

    @Override
    public String toString() {
        return "MaterializerConfig{"
                + "name='"
                + name
                + '\''
                + ", kafkaConfig="
                + kafkaConfig
                + ", databaseRestServiceConfig="
                + databaseRestServiceConfig
                + '}';
    }

    public static final class KafkaConfig {
        public final String bootstrapServers;
        public final String schemaRegistryUrl;
        public final String sourceTopic;

        public KafkaConfig(String bootstrapServers, String schemaRegistryUrl, String sourceTopic) {
            this.bootstrapServers = bootstrapServers;
            this.schemaRegistryUrl = schemaRegistryUrl;
            this.sourceTopic = sourceTopic;
        }

        @Override
        public String toString() {
            return "KafkaConfig{"
                    + "bootstrapServers='"
                    + bootstrapServers
                    + '\''
                    + ", schemaRegistryUrl='"
                    + schemaRegistryUrl
                    + '\''
                    + ", sourceTopic='"
                    + sourceTopic
                    + '\''
                    + '}';
        }
    }

    public static final class DatabaseRestServiceConfig {
        public final String url;

        public DatabaseRestServiceConfig(String url) {
            this.url = url;
        }

        @Override
        public String toString() {
            return "DatabaseRestServiceConfig{" + "url='" + url + '\'' + '}';
        }
    }
}
