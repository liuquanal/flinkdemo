package com.aquanz.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 基于Kafka的词频统计
 *
 * @author a.q.z 2019/10/16 上午12:52
 */
public class KafkaWordCount {

    private final static String KAFKA_SERVER = "localhost:9092";
    private final static String KAFKA_ZOOKEEPER = "localhost:2181";
    private final static String KAFKA_GROUP = "jk-c";
    private final static String KAFKA_TOPIC = "jk";

    private final static String REDIS_SERVER = "127.0.0.1";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);
        properties.setProperty("zookeeper.connect", KAFKA_ZOOKEEPER);
        properties.setProperty("group.id", KAFKA_GROUP);

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(KAFKA_TOPIC,
                new SimpleStringSchema(), properties);

        DataStream<String> stream = env.addSource(myConsumer);
        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new LineSplitter())
                .keyBy(0).sum(1);

        // 打印结果
        System.out.println("word count:");
        counts.print().setParallelism(1);

        // 插入redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(REDIS_SERVER).build();
        counts.addSink(new RedisSink<>(conf, new RedisExampleMapper()));

        env.execute("WordCount from Kafka");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    public static final class RedisExampleMapper implements RedisMapper<Tuple2<String, Integer>> {
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "words");
        }

        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }
}
