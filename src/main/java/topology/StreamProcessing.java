package topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamProcessing {

    // каждый запрос уникален
    // конвертирует каждое сообщение в заглавные буквы
    public static Topology topologyUpperCase(String sourceTopic, String sinkTopic) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> sourceStream =
                streamsBuilder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()));

        sourceStream
                .mapValues((ValueMapper<String, String>) String::toUpperCase)
                .to(sinkTopic, Produced.with(Serdes.String(), Serdes.String()));
        return streamsBuilder.build();
    }

    // сохраняет данные при работе как состояние внутри себя
    // считает количество анаграм при обработки сообщений
    public static Topology topologyCountAnagram(String sourceTopic, String sinkTopic, String storeName) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> sourceStream =
                streamsBuilder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()));
        // 1. [null:"magic"] => ["acgim":"magic"]
        // 2. сумма значений с одинаковым ключом
        sourceStream
                .map(
                        (key, value) -> {
                            final String newKey =
                                    Stream.of(value.replaceAll(" ", "").split(""))
                                            .sorted()
                                            .collect(Collectors.joining());
                            return KeyValue.pair(newKey, value);
                        })
                .groupByKey()
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName))
                .toStream()
                .to(sinkTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return streamsBuilder.build();
    }
}
