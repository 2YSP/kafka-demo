import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by 2YSP on 2018/4/3.
 */
public class ConsumerGroup {

    public static void main(String[] args) {
        if (args.length < 2){
            System.out.println("Usage: consumer <topic> <groupname> ");
            return;
        }


        String topic = args[0];
        String group = args[1];

        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.75.132:9092");
        props.put("group.id",group);
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");
        props.put("session.timeout.ms","30000");
        //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(200);
            for(ConsumerRecord<String, String> record:records){
                System.out.printf("============offset = %d,key = %s,value=%s\n",record.offset(),record.key(),record.value());
            }
            //提交已经拉取出来的offset,如果是手动模式下面,必须拉取之后提交,否则以后会拉取重复消息
            consumer.commitSync();

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
