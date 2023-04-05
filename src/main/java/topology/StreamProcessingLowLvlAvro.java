package topology;

import avroModels.Person;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

public class StreamProcessingLowLvlAvro {

    // сохраняет данные при работе как состояние внутри себя
    // дедуплицирует данные по айди юзера (удаляет повторяющиеся значения)
    public static Topology topologyDedupByUserId(String sourceTopic, String sinkTopic, Serde<Person> personSerdes,
                                                 String idStore) {

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .addStateStore(
                        Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(idStore), Serdes.String(), personSerdes))
                .stream(sourceTopic, Consumed.with(Serdes.String(), personSerdes))
                .transform(
                        () ->
                                new Transformer<String, Person, KeyValue<String, Person>>() {
                                    KeyValueStore<String, Person> stateStore;

                                    @Override
                                    public void init(ProcessorContext context) {
                                        this.stateStore = (KeyValueStore<String, Person>) context.getStateStore(idStore);
                                    }

                                    @Override
                                    public KeyValue<String, Person> transform(String key, Person value) {
                                        String id = value.getId();
                                        if (!id.equals(key)) return null; // да

                                        Person person = stateStore.get(key);
                                        if (person == null) {
                                            // добавляем в хранилище
                                            stateStore.put(key, value);
                                            return KeyValue.pair(key, value);
                                        } else {
                                            return null;
                                        }
                                    }

                                    @Override
                                    public void close() {
                                    }
                                },
                        idStore)
                .to(sinkTopic, Produced.with(Serdes.String(), personSerdes));
        return builder.build();
    }
}
