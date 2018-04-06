import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by 2YSP on 2018/4/3.
 */
public class ProducerTest {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Enter topic name");
            return;
        }

        //Assign topicName to string variable
        String topicName = args[0];

        //create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id 192.168.75.132
        props.put("bootstrap.servers","192.168.75.132:9092");

        props.put("acks","all");

        //If the request fails, the producer can automatically retry,
        props.put("retries",0);

        //Specify buffer size in config
        props.put("batch.size",16384);

        props.put("buffer.memory",33554432);

        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String, String>(props);
        for (int i=0;i<10;i++){
            producer.send(new ProducerRecord<String, String>(topicName,Integer.toString(i),Integer.toString(i)));

        }
        System.out.println("message send successfully");
        producer.close();
    }
}
