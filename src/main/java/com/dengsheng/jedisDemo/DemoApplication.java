package com.dengsheng.jedisDemo;

import java.util.Arrays;
import java.util.List;

import com.dengsheng.jedisDemo.util.RedisClusterUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.util.JedisClusterCRC16;



//@SpringBootApplication
public class DemoApplication {
	
	

	public static void main(String[] args) {
		
//		RedisClusterUtil.jedisCluster.set("hello", "world");
//		System.out.println(RedisClusterUtil.jedisCluster.get("key"));
		
//		String pattern = "key*";
//		int scanCounter = 10;
//		RedisClusterUtil.delRedisClusterByPattern(pattern);
//		System.out.println(RedisClusterUtil.keysRedisClusterByPattern(pattern, scanCounter));
		

		
/*		使用Lua、事务等特性的方法,Lua和事务需要所操作的key，必须在一个节点上，不过Redis Cluster提
		供了hashtag，如果开发人员确实要使用Lua或者事务，可以将所要操作的key
		使用一个hashtag，*/
		// hashtag
		String hastag = "{user}";
		// 用户A的关注表
		String userAFollowKey = hastag + ":a:follow";
		// 用户B的粉丝表
		String userBFanKey = hastag + ":b:fans";
		// 计算hashtag对应的slot
		int slot = JedisClusterCRC16.getSlot(hastag);
		// 获取指定slot的JedisPool
		Jedis jedis = RedisClusterUtil.jedisCluster.getConnectionFromSlot(slot);
		// 在当个节点上执行事务
//		Jedis jedis = null;
		try {
//			jedis = jedisPool.getResource();
			// 用户A的关注表加入用户B，用户B的粉丝列表加入用户A
			Transaction transaction = jedis.multi();
			transaction.sadd(userAFollowKey, "user:b");
			transaction.sadd(userBFanKey, "user:a");
			transaction.exec();
		} catch (Exception e) {
//			logger.error(e.getMessage(), e);
		} finally {
			if (jedis != null)
				jedis.close();
		}
	
//		SpringApplication.run(DemoApplication.class, args);
	}
	
	
	


}
