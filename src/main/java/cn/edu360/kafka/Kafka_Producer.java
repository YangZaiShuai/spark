package cn.edu360.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class Kafka_Producer {
    public static void main(String[] args) {

        //生产者不需要知道zk
        Properties props = new Properties();
        props.put("metadata.broker.list", "hdp-01:9092,hdp-02:9092,hdp-03:9092");
        //指定序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);

        for (int i=100;i<200;i++)
            producer.send(new KeyedMessage<String,String>("wc","xiaoniu"+i));

    }
}
