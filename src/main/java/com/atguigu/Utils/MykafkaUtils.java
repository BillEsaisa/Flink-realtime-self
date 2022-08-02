package com.atguigu.Utils;

import com.atguigu.common.Gmallconfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.map.module.SimpleDeserializers;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @PackageName:com.atguigu.Utils
 * @ClassNmae:MykafkaUtils
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/18 22:42
 */


public class MykafkaUtils {

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String Topic, String groupid) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(Topic, new KafkaDeserializationSchema<String>() {

            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if (record != null && record.value() != null) {
                    return new String(record.value());
                }

                return null;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        }, properties);


        return kafkaConsumer;


    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", Gmallconfig.BROKE_LIST);
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 15 * 1000 + "");

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(topic, new KafkaSerializationSchema<String>() {

            @Override
            public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(topic, jsonStr.getBytes());
            }
        }, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        return producer;
    }


}




