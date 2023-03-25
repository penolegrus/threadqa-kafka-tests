package producer.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import producer.domain.MessageModel;
import producer.domain.MessageTransformer;
import producer.kafka.KafkaMessageProducer;
import org.apache.avro.AvroRuntimeException;

@Path("messages")
@Produces(MediaType.APPLICATION_JSON)
public class MessageResources {
    private final KafkaMessageProducer messageProducer;
    private final MessageTransformer transformer;

    public MessageResources(KafkaMessageProducer messageProducer, MessageTransformer transformer) {
        this.messageProducer = messageProducer;
        this.transformer = transformer;
    }

    @GET
    public Response healthCheck() {
        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response produceAsync(final MessageModel messageModel) {
        try {
            messageProducer.producerMessage(transformer.transform(messageModel));
            return Response.accepted().build();
        } catch (AvroRuntimeException exception) {
            return Response.status(400).entity(exception.toString()).build();
        }
    }
}
