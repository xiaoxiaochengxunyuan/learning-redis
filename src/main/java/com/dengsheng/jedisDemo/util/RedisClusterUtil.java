package com.dengsheng.jedisDemo.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;


public class RedisClusterUtil {
	
	public static JedisCluster jedisCluster = getConnect();
	
	private RedisClusterUtil() {};
	
	private static JedisCluster getConnect() {
		if(jedisCluster == null) {
			synchronized (RedisClusterUtil.class) {
				if(jedisCluster == null) {
					// 初始化所有节点(例如6个节点)
					Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
					jedisClusterNode.add(new HostAndPort("127.0.0.1", 6379));
					jedisClusterNode.add(new HostAndPort("127.0.0.1", 6380));
					jedisClusterNode.add(new HostAndPort("127.0.0.1", 6382));
					jedisClusterNode.add(new HostAndPort("127.0.0.1", 6383));
					jedisClusterNode.add(new HostAndPort("127.0.0.1", 6385));
					jedisClusterNode.add(new HostAndPort("127.0.0.1", 6386));
					// 初始化commnon-pool连接池，并设置相关参数
					GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
					// 初始化JedisCluster
					jedisCluster = new JedisCluster(jedisClusterNode, 1000, 1000, 5, poolConfig);
				}
			}
		}
		return jedisCluster;
	}
	
	public static void delRedisClusterByPattern(String pattern) {
		delRedisClusterByPattern(pattern, 3);
	}
	
	// 从RedisCluster批量删除指定pattern的数据
	public static void delRedisClusterByPattern(String pattern, int scanCounter) {
		// 获取所有节点的JedisPool
		Map<String, JedisPool> jedisPoolMap = jedisCluster.getClusterNodes();
		for (Entry<String, JedisPool> entry : jedisPoolMap.entrySet()) {
			// 获取每个节点的Jedis连接
			Jedis jedis = entry.getValue().getResource();
			// 只删除主节点数据
			if (!isMaster(jedis)) {
				continue;
			}
			// 使用Pipeline每次删除指定前缀的数据
			Pipeline pipeline = jedis.pipelined();
			// 使用scan扫描指定前缀的数据
			String cursor = "0";
			// 指定扫描参数：每次扫描个数和pattern
			ScanParams params = new ScanParams().count(scanCounter).match(pattern);
			while (true) {
				// 执行扫描
				ScanResult<String> scanResult = jedis.scan(cursor, params);
				// 删除的key列表
				List<String> keyList = scanResult.getResult();
				if (keyList != null && keyList.size() > 0) {
					for (String key : keyList) {
						pipeline.del(key);
					}
					// 批量删除
					pipeline.syncAndReturnAll();
				}
				cursor = scanResult.getCursor();
				// 如果游标变为0，说明扫描完毕
				if ("0".equals(cursor)) 	{
					break;
				}
			}
		}
	}
	
	// 从RedisCluster批量删除指定pattern的数据
		public static List<Object> keysRedisClusterByPattern(String pattern, int scanCounter) {
			List<Object> result = new ArrayList<>();
			// 获取所有节点的JedisPool
			Map<String, JedisPool> jedisPoolMap = jedisCluster.getClusterNodes();
			for (Entry<String, JedisPool> entry : jedisPoolMap.entrySet()) {
				// 获取每个节点的Jedis连接
				Jedis jedis = entry.getValue().getResource();
				// 只删除主节点数据
				if (!isMaster(jedis)) {
					continue;
				}
				// 使用Pipeline每次删除指定前缀的数据
				Pipeline pipeline = jedis.pipelined();
				// 使用scan扫描指定前缀的数据
				String cursor = "0";
				// 指定扫描参数：每次扫描个数和pattern
				ScanParams params = new ScanParams().count(scanCounter).match(pattern);
				while (true) {
					// 执行扫描
					ScanResult<String> scanResult = jedis.scan(cursor, params);
					// 删除的key列表
					List<String> keyList = scanResult.getResult();
//					if (keyList != null && keyList.size() > 0) {
//						for (String key : keyList) {
//							pipeline.get(key);
//						}
//						// 批量获取
//						result = pipeline.syncAndReturnAll();
//					}
					result.addAll(keyList);
					cursor = scanResult.getCursor();
					// 如果游标变为0，说明扫描完毕
					if ("0".equals(cursor)) 	{
						break;
					}
				}
			}
			return result;
		}

	// 判断当前Redis是否为master节点
	private static boolean isMaster(Jedis jedis) {
		String[] data = jedis.info("Replication").split("\r\n");
		for (String line : data) {
			if ("role:master".equals(line.trim())) {
				return true;
			}
		}
		return false;
	}
}
