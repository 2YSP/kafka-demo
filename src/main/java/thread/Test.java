package thread;

/**
 * @Author: 2YSP
 * @Description:
 * @Date: Created in 2018/4/11
 */
public class Test {

    private static final int threadNum = 3;
    private static final String topic = "test";
    private static final String groupId = "group1";
    private static final String brokerList = "192.168.75.132:9092";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        for(int i=0;i<threadNum;i++){
            Thread t = new Thread(()-> new ConsumerRunnable(topic,groupId,brokerList));
            t.start();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("===用时："+(endTime-startTime));
    }
}
