package com.treutec.events

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import java.util.Properties

// $ kafka-topics --zookeeper localhost:2181 --create --topic ages --replication-factor 1 --partitions 4

fun main(args: Array<String>) {
    EventProducer("localhost:9092").process()
}

class EventProducer(val brokers: String) {

    fun process() {
        val streamsBuilder = StreamsBuilder()

        val eventStream: KStream<String, String> = streamsBuilder
                .stream("events", Consumed.with(Serdes.String(), Serdes.String()))

//        val a: KStream<String, String> = eventStream.selectKey { k, v ->
//            println("$k,$v")
//            "plop"
//        }
//
//        a.to("plop", Produced.with(Serdes.String(), Serdes.String()))

        val aggregates: KTable<Windowed<String>, Long> = eventStream
                .groupBy({ k, v -> "dummy" }, Serialized.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(10000))
//                .windowedBy(TimeWindows.of(10000).grace(10000))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))

        aggregates
//                .suppress(intermediateEvents(withEmitAfter(Duration.ofSeconds(10))))
//                .suppress(emitFinalResultsOnly(withBufferFullStrategy(SHUT_DOWN)))
                .toStream()
                .map { ws, i -> KeyValue("${ws.window().start()}", "$i") }
                .to("aggs", Produced.with(Serdes.String(), Serdes.String()))

        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-tutorial-events-4"
        props["auto.offset.reset"] = "latest"
        props["commit.interval.ms"] = 0
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}