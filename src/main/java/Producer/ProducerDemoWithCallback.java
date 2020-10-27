package Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

  public static void main(String[] args) {

    final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    // step1: create producer properties
    Properties properties = new Properties();
    String bootstrapServers = "127.0.0.1:9092";
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // step2: create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    // create a producer record
    ProducerRecord<String, String> record =
        new ProducerRecord<String, String>("first_topic", "hello world");

    // step3: send data - asynchronous
    producer.send(
        record,
        new Callback() {
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            // executes every time a record is successfully sent or an exception is thrown
            if (e == null) {
              logger.info(
                  "Received new metedata. \n"
                      + "Topic:"
                      + recordMetadata.topic()
                      + "\n"
                      + "Partition: \n"
                      + recordMetadata.partition()
                      + "Offset: "
                      + recordMetadata.offset()
                      + "\n"
                      + "Timestamp: "
                      + recordMetadata.timestamp());
            } else {
              logger.error("Error while producing", e);
            }
          }
        });

    // flush and close producer
    producer.flush();
    producer.close();
  }
}
