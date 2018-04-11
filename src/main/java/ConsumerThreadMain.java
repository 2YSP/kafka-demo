/**
 * @Author: 2YSP
 * @Description:
 * @Date: Created in 2018/4/10
 */
public class ConsumerThreadMain {
    private static final String brokerList = "localhost:9092";
    private static final String groupId = "group1";
    private static final String topic = "test";
    private static final int threadNum = 3;

    public static void main(String[] args) {
        ConsumerGroup2 group2 = new ConsumerGroup2(threadNum,groupId,topic,brokerList);
        group2.execute();
    }
}
