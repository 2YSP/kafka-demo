package thread;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author: 2YSP
 * @Description:
 * @Date: Created in 2018/4/11
 */
public class ConsumerRunnable implements Runnable{

    private final Consumer<String,String> consumer;

    private static Logger logger= LoggerFactory.getLogger(ConsumerRunnable.class);

    public ConsumerRunnable(String topic, String groupId, String brokerList){
        Properties props = new Properties();
        props.put("bootstrap.servers",brokerList);
        props.put("group.id",groupId);
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");
        props.put("session.timeout.ms","30000");
        //当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() <= 0){
                continue;
            }
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
