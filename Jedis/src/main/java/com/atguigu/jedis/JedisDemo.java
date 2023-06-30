package com.atguigu.jedis;

import jdk.nashorn.internal.objects.annotations.Where;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ListPosition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JedisDemo {

    public static String hostname = "hadoop102";
    public static Integer port = 6379;
    private static JedisPool jedisPool = null;

    public static void main(String[] args) {


    }

    public static void testString() {
        Jedis jedis = getJedisFromPool();

        jedis.set("color", "red");
        System.out.println(jedis.get("color"));

        jedis.append("color", ",white");
        System.out.println(jedis.get("color"));

        System.out.println(jedis.strlen("color"));

        System.out.println(jedis.setnx("color", "black"));
        System.out.println(jedis.setnx("user", "1001"));

        jedis.incr("user");
        System.out.println(jedis.get("user"));

        jedis.decr("user");
        System.out.println(jedis.get("user"));

        jedis.incrBy("user", 5);
        System.out.println(jedis.get("user"));

        jedis.decrBy("user", 5);
        System.out.println(jedis.get("user"));

        jedis.mset("k1", "v1", "k2", "v2", "k3", "v3");
        List<String> mget = jedis.mget("k1", "k2", "k3");
        System.out.println("mget = " + mget);

        jedis.msetnx("k4", "v4", "k5", "v5");

        jedis.getrange("color", 1, 3);

        jedis.setrange("color", 0, "sex");

        jedis.setex("t1", 10, "time");

        jedis.getSet("k1", "value1");

    }

    public static void testList() {

        Jedis jedis = getJedisFromPool();

        jedis.lpush("list", "1", "2", "3");

        jedis.rpush("list", "a", "b", "c");

        jedis.lpop("list");

        jedis.rpop("list");

        jedis.rpoplpush("list", "list");

        jedis.lrange("list", 1, -1);

        jedis.lindex("list", 3);

        jedis.llen("list");

        jedis.linsert("list", ListPosition.BEFORE, "A", "a");

        jedis.lrem("list", 2, "a");

    }

    public static void testSet() {

        Jedis jedis = getJedisFromPool();

        jedis.sadd("set", "1", "2", "3", "4");
        jedis.sadd("set2", "3", "4", "a");

        jedis.smembers("set");

        jedis.sismember("set", "a");

        jedis.scard("set");

        jedis.srem("set", "1");

        jedis.spop("set2");

        jedis.srandmember("set");

        jedis.sinter("set", "set2");

        jedis.sunion("set", "set2");

        jedis.sdiff("set", "set2");

    }

    public static void testZset() {

        Jedis jedis = getJedisFromPool();

        Map<String, Double> map = new HashMap<>();

        map.put("faf", 20d);
        map.put("gdf", 30d);
        map.put("hjf", 15d);

        jedis.zadd("zset", 10, "ad");
        jedis.zadd("zset", map);

        jedis.zrange("zset",0,-1);

        jedis.zrevrange("zest",0,-1);

        jedis.zrangeByScore("zset",10,40);

        jedis.zrevrangeByScore("zset",40,10);

        jedis.zincrby("zset",10,"ad");

        jedis.zrem("zset","ad");

        jedis.zcount("zset",10,50);

        jedis.zrank("zset","faf");

    }

    public static void testHash() {

        Jedis jedis = getJedisFromPool();

        jedis.hset("hset","f1","v1");

        jedis.hsetnx("hset","f2","v2");

        jedis.hget("hset","f2");

        jedis.hexists("hset","f1");

        jedis.hkeys("hset");

        jedis.hvals("hset");

        jedis.hincrBy("hset","f2",3);

    }

    /**
     * 基于连接池获取Jedis
     *
     * @return
     */
    public static Jedis getJedisFromPool() {
        if (jedisPool == null) {
            //创建连接池
            //主要配置
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxIdle(10);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(2000);
            jedisPoolConfig.setTestOnBorrow(true);
            jedisPool = new JedisPool(jedisPoolConfig, hostname, port);
        }
        //从池中获取连接
        return jedisPool.getResource();
    }

}
