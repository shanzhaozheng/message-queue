package szzii.com.messagequeue.notify.strategy;

import java.util.List;

/**
 * 缓存策略接口
 * 目的:发送模板与依赖的缓存中间件解耦。
 * 缺陷:消息消费方法{@link CacheStrategy#consumerMessage()} 需要获取数据并删除，
 *      推送消息期间数据会一直保存在内存当中,如果此时当前应用挂掉数据则丢失。
 * @author szz
 */
public interface CacheStrategy<T> {

    /**
     * 生产消息
     * @param obj
     * @return
     */
    void productionMessage(T obj);

    /**
     * 消费消息
     * @return
     */
    List<T> consumerMessage();

    /**
     * 消费消息指定数量
     * @return
     */
    List<T> consumerMessage(Integer amount);


    /**
     * 获取缓存中消息数量,并不重要.
     * @return
     */
    Long residue();


}
