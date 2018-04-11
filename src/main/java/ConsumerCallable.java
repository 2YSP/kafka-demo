import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: 2YSP
 * @Description:
 * @Date: Created in 2018/4/10
 */
public class ConsumerCallable implements Callable<ConsumerFuture> {

    private static Logger logger= LoggerFactory.getLogger(ConsumerCallable.class);

    private AtomicInteger totalCount = new AtomicInteger();
    private AtomicLong totalTime= new AtomicLong();

    private AtomicInteger count = new AtomicInteger();
    /**
     * 每个线程维护kafkaConsumer实例
     */
    private final KafkaConsumer<String,String> consumer;

    public ConsumerCallable(String brokerList,String groupId,String topic){
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
        this.consumer = new KafkaConsumer<String, String>(props);
        this.consumer.subscribe(Arrays.asList(topic));
    }
    @Override
    public ConsumerFuture call() throws Exception {
        boolean flag = true;
        int failPollTimes = 0;
        long startTime = System.currentTimeMillis();

        while (flag){
            ConsumerRecords<String, String> records = consumer.poll(200);
            if(records.count() <= 0){
                failPollTimes++;

                if (failPollTimes >= 20){
                    logger.debug("失败达到{}次数，退出",failPollTimes);
                    flag = false;
                }
                continue;
            }
            //获取到后则清零
            failPollTimes=0;
            logger.info("本次获取:"+records.count());
            count.addAndGet(records.count());
            totalCount.addAndGet(count.get());
            long endTime = System.currentTimeMillis();
            if(count.get() >= 10000){
                logger.info("this consumer {} records,user {} milliseconds",count,endTime-startTime);
                totalTime.addAndGet(endTime-startTime);
                startTime = System.currentTimeMillis();
                count = new AtomicInteger();
            }
            logger.info("end totalCount={},min={}",totalCount,totalTime);

        }

        ConsumerFuture future = new ConsumerFuture(totalCount.get(),totalTime.get());
        return future;
    }
}
