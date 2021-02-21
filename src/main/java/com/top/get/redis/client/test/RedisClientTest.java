package com.top.get.redis.client.test;

import redis.clients.jedis.Jedis;

public class RedisClientTest {
	
	public static String testRedis() {
		Jedis jedis = new Jedis("34.122.123.235",6379);
		jedis.auth("RedisJuse@2KDP");
		System.out.println("jedis started succesfully");
		jedis.set("post/1", "{\"postId\":1}");
		System.out.println("Get Jedis:"+jedis.get("post/1"));
		jedis.close();
		return null;
	}
	
	public static void main(String[] args) {
		testRedis();
	}
}
