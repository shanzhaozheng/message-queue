package szzii.com.messagequeue.notify.strategy;

import java.util.List;

/**
 * redis 消息队列模板
 * 需实现消息推送策略即可 {@link AbstractNotify#sendMessages(Object)},注意此方法返回值是判断消息是否发送成功的唯一判断
 * - 此类既是模板也是一个桥接类,对两个维度进行扩展
 *    1. 发送端: 对不同接收者进行数据封装以及发送策略
 *    2. 缓存策略: 对不同发送失败消息进行不同的缓存策略,目前仅对不同缓存中间件进行解耦,如 memecached、redis、本地缓存等。
 * @author szz
 */
public abstract class AbstractNotify<T>{

    private CacheStrategy cacheStrategy;


    public AbstractNotify(CacheStrategy cacheStrategy) {
        this.cacheStrategy = cacheStrategy;
    }

    /**
     * 一般消息推送策略
     * @param obj 推送消息
     * @return 返回值决定是否推送成功以用来判断是否进行缓存
     */
    protected abstract boolean sendMessages(T obj);

    /**
     * 模板方法
     * 推送消息,将缓存中消息重新推送
     * @param amount 推送数量
     * @return 队列剩余消息数量
     */
    public final Long reSendMessage(Integer amount){
        sendMessages(consumerMessage(amount));
        return cacheStrategy.residue();
    }


    /**
     * 模板方法
     * 推送消息,将缓存中消息重新推送
     * @return 队列剩余消息数量
     */
    public final Long reSendMessage(){
        sendMessages(consumerMessage());
        return cacheStrategy.residue();
    }


    /**
     * 推送缓存消息策略
     * @param obj 推送消息
     * @return 返回值确定此次消息是否正常推送成功
     */
    private Boolean sendMessages(List<T> obj){
        if (null == obj) return true;
        for (T t : obj) {
            if (!sendMessages(t)){
                productionMessage(t);
            }
        }
        return true;
    }


    /**
     * 生产消息
     * @param obj
     * @return
     */
    public final void productionMessage(T obj){
        cacheStrategy.productionMessage(obj);
    }

    /**
     * 生产消息
     * @param obj
     * @return
     */
    public final void sendMessageNow(T obj){
        boolean flag = this.sendMessages(obj);
        if (!flag){
            productionMessage(obj);
        }
    }

    public List<T> consumerMessage(Integer amount) {
        return cacheStrategy.consumerMessage(amount);
    }

    public List<T> consumerMessage() {
        return cacheStrategy.consumerMessage();
    }
}
