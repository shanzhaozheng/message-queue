package szzii.com.messagequeue.notify.extedCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import szzii.com.messagequeue.notify.strategy.CacheStrategy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * redis 消息消费缓存策略
 * @author szz
 */
public class RedisStrategy implements CacheStrategy {

    private static final Logger log = LoggerFactory.getLogger(RedisStrategy.class);

    private RedisTemplate redisTemplate;

    private ThreadPoolExecutor poolExecutor;

    private String cacheKey;

    private int retryMaxCount;


    public RedisStrategy(RedisTemplate redisTemplate, ThreadPoolExecutor poolExecutor, String cacheKey) {
        this(redisTemplate,poolExecutor, cacheKey,10);
    }

    public RedisStrategy(RedisTemplate redisTemplate, ThreadPoolExecutor poolExecutor, String cacheKey, int retryMaxCount) {
        this.redisTemplate = redisTemplate;
        this.poolExecutor = poolExecutor;
        this.cacheKey = cacheKey;
        this.retryMaxCount = retryMaxCount;
    }

    @Override
    public void productionMessage(Object obj) {
        if (null == obj) return;
        CompletableFuture.runAsync(() -> {
            int retryCount = 0;
            Long result = null;
            do {
                retryCount++;
                try {
                    result = redisTemplate.opsForList().leftPush(cacheKey, obj);
                } catch (Exception e) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(500);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                        log.error("redis : 生产数据异常中断");
                        Thread.currentThread().interrupt();
                    }
                    log.error(String.format("redis : 当前线程 %s 存入消息异常重试 %d 次",Thread.currentThread().getName(),retryCount));
                }
            } while ((result == null || result <= 0) && retryCount < retryMaxCount);
        },poolExecutor);
    }

    @Override
    public List consumerMessage() {
        List list = null;
        Long size = redisTemplate.opsForList().size(cacheKey);
        if (null == size || size <= 0) return list;
        try {
            list = redisTemplate.executePipelined((RedisCallback<?>) redisConnection -> {
                for (int i = 0; i < size; i++) {
                    redisConnection.rPop((cacheKey).getBytes());
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
            log.error("redis : 消费数据异常中断");
        }
        return list;
    }

    @Override
    public List consumerMessage(Integer amount) {
        if (null == amount || amount <= 0) {
            return consumerMessage();
        }
        List list = null;
        Long size = redisTemplate.opsForList().size(cacheKey);
        try {
            list = redisTemplate.executePipelined((RedisCallback<?>) redisConnection -> {
                for (int i = 0; i < size; i++) {
                    redisConnection.rPop((cacheKey).getBytes());
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
            log.error("redis : 消费数据异常中断");
        }
        return list;
    }

    @Override
    public Long residue() {
        return redisTemplate.opsForList().size(cacheKey);
    }


}

