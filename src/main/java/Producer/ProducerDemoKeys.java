package Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  public static void main(String[] args) throws ExecutionException, InterruptedException {

    final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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

    for (int i = 0; i < 10; i++) {
      // step3: send data - asynchronous

      String topic = "first_topic";
      String value = "hello_world " + i;
      String key = "id_" + i;

      // create a producer record
      final ProducerRecord<String, String> record =
          new ProducerRecord<String, String>(topic, key, value);

      logger.info("key: " + key);

      producer
          .send(
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
              })
          .get();
    }

    // flush and close producer
    producer.flush();
    producer.close();
  }
}
