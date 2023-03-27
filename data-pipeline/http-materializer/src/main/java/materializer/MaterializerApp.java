package materializer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import materializer.domain.MessageTransformer;
import materializer.infrastructure.kafka.KafkaMessageTransformer;
import materializer.infrastructure.service.DatabaseWebService;
import materializer.infrastructure.service.DatabaseWebServiceInMemmory;
import materializer.infrastructure.service.DatabaseWebServiceRest;

public class MaterializerApp {

    private final DatabaseWebService dbWebService;
    private final MessageTransformer transformer;
    private final KafkaMessageTransformer kafkaMessageTransformer;

    public MaterializerApp(MaterializerConfig materializerConfig, MessageTransformer transformer, boolean inMemory) {
        this.dbWebService = inMemory ? new DatabaseWebServiceInMemmory() : new DatabaseWebServiceRest(materializerConfig);
        this.transformer = new MessageTransformer();
        this.kafkaMessageTransformer = new KafkaMessageTransformer(materializerConfig, dbWebService, transformer);
    }

    public static void main(String[] args) {
        // загружаем конфиг из ресурсов
        final Config config = ConfigFactory.load();
        final MaterializerConfig materializerConfig = MaterializerConfig.loadConfig(config);
        final MessageTransformer messageTransformer = new MessageTransformer();

        final MaterializerApp materializerApp = new MaterializerApp(materializerConfig, messageTransformer, false);

        materializerApp.start();
    }

    public void stop() {
        kafkaMessageTransformer.stop();
    }

    public void start() {
        kafkaMessageTransformer.run();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaMessageTransformer::stop));
    }

    public DatabaseWebService getDbWebService() {
        return dbWebService;
    }

    public KafkaMessageTransformer getKafkaMessageMaterializer() {
        return kafkaMessageTransformer;
    }
}
