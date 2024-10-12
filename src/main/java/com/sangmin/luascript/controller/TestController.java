package com.sangmin.luascript.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

@Log4j2
@RestController
@RequiredArgsConstructor
public class TestController {

    private static final String QUEUE = "queue";
    private static final String LUA_SCRIPT = """
            local queueKey = KEYS[1]
            local size = redis.call('ZCARD', queueKey)
            if size >= 4 then
                local userIds = redis.call('ZRANGE', queueKey, 0, 3)
                redis.call('ZREMRANGEBYRANK', queueKey, 0, 3)
                return {size, userIds}
            else
                return {size}
            end
            """;

    private final StringRedisTemplate stringRedisTemplate;
    private final Set<String> mySet = new HashSet<>();


    @GetMapping("/test")
    public void test() {
        double currentTime = Instant.now().toEpochMilli();
        String next = String.valueOf(ThreadLocalRandom.current().nextInt());
        stringRedisTemplate.opsForZSet().add(QUEUE, next, currentTime);
        log.info("next: {}", next);

        List result = stringRedisTemplate.execute(
                new DefaultRedisScript<>(LUA_SCRIPT, List.class),
                List.of(QUEUE)
        );

        log.info("result: {}", result);
        assert result != null;
        if (result.size() == 2) {
            List<String> o = (List<String>) result.get(1);
            mySet.addAll(o);
        }
    }

    @GetMapping("/get")
    public int get() {
        return mySet.size();
    }

}
