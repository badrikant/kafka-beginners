package com.learn.kafka.sample;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author badrikant.soni on Oct,2018 at 4:23 PM
 */
public class ProducerCallBack {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerCallBack.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            // create a producer record
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic", "hello World" + Integer.toString(i));

            // send data - asynchronous
            producer.send(record, new Callback() {

                // execute every time a record is successfully send or an exception is thrown.
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topics : " + recordMetadata.topic() + "\n" +
                                "Partition :" + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "TimeStamp : " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing " + e);
                    }
                }
            });

        }

        // flush
        producer.flush();

        // flush & close
        producer.close();

    }
}
