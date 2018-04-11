import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Author: 2YSP
 * @Description:
 * @Date: Created in 2018/4/10
 */
public class ConsumerGroup2 {

    private static Logger logger= LoggerFactory.getLogger(ConsumerGroup2.class);
    /**
     * 线程池
     */
    private ExecutorService threadPool;

    private List<ConsumerCallable> consumers;

    public ConsumerGroup2(int threadNum,String groupId,String topic,String brokerList){
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("consumer-pool-%d").build();

        threadPool = new ThreadPoolExecutor(threadNum,threadNum,0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(1024),namedThreadFactory,new ThreadPoolExecutor.AbortPolicy());

        consumers = new ArrayList<>(threadNum);
        for(int i=0;i<threadNum;i++){
            ConsumerCallable consumer = new ConsumerCallable(brokerList,groupId,topic);
            consumers.add(consumer);
        }
    }

    /**
     * 执行任务
     */
    public void execute(){
        long start = System.currentTimeMillis();
        for(ConsumerCallable runnable:consumers){
            Future<ConsumerFuture> future = threadPool.submit(runnable);
        }
        if (threadPool.isShutdown()){
            long end = System.currentTimeMillis();
            logger.info("main thread use {} millis",end-start);
        }
        threadPool.shutdown();
    }
}
