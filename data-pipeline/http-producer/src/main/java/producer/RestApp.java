package producer;


import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import producer.domain.MessageTransformer;
import producer.kafka.KafkaMessageProducer;
import producer.rest.MessageResources;

public class RestApp extends io.dropwizard.Application<ProducerRestConfig>{


  public static void main(String[] args) throws Exception {
    new RestApp().run(args);
  }

  // подключаем переменные окружения в замоканное приложение
  @Override
  public void initialize(Bootstrap<ProducerRestConfig> bootstrap) {
    bootstrap.setConfigurationSourceProvider(
            new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                    new EnvironmentVariableSubstitutor(false)));
    super.initialize(bootstrap);
  }

  @Override // надо переопределить логи иначе не запустится
  protected void bootstrapLogging() {
  }

  @Override
  public void run(ProducerRestConfig producerRestConfig, Environment environment) {
    environment
            .healthChecks()
            .register(producerRestConfig.getName() + "HealthCheck", new ApplicationHealthCheck());

    KafkaMessageProducer messageProducer = new KafkaMessageProducer(producerRestConfig);
    MessageTransformer transformer = new MessageTransformer();
    MessageResources messageResources = new MessageResources(messageProducer, transformer);

    // регаем эндпоинты и остальную часть для запуска приложения
    environment.jersey().register(messageResources);
  }

}
