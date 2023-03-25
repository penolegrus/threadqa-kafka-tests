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

  // enable environment variables
  @Override
  public void initialize(Bootstrap<ProducerRestConfig> bootstrap) {
    bootstrap.setConfigurationSourceProvider(
            new SubstitutingSourceProvider(
                    bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor(false)));
    super.initialize(bootstrap);
  }

  @Override
  public void run(ProducerRestConfig producerRestConfig, Environment environment) {
    environment
            .healthChecks()
            .register(producerRestConfig.getName() + "HealthCheck", new ApplicationHealthCheck());

    KafkaMessageProducer messageProducer = new KafkaMessageProducer(producerRestConfig);
    MessageTransformer transformer = new MessageTransformer();
    MessageResources messageResources = new MessageResources(messageProducer, transformer);

    // register REST endpoints
    environment.jersey().register(messageResources);
  }

}
