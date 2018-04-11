/**
 * @Author: 2YSP
 * @Description:
 * @Date: Created in 2018/4/10
 */
public class ConsumerFuture {
    private Integer totalCount;
    private Long totalTime;

    public ConsumerFuture(Integer totalCount, Long totalTime) {
        this.totalCount = totalCount;
        this.totalTime = totalTime;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public Long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(Long totalTime) {
        this.totalTime = totalTime;
    }
}
