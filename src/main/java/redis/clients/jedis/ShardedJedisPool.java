package redis.clients.jedis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.util.Hashing;
import redis.clients.util.Pool;
import redis.clients.util.ShardInfo;

public class ShardedJedisPool extends Pool<ShardedJedis> {
	public ShardedJedisPool(final GenericObjectPool.Config poolConfig,
			List<JedisShardInfo> shards) {
		this(poolConfig, shards, Hashing.MURMUR_HASH);
	}

	public ShardedJedisPool(final GenericObjectPool.Config poolConfig,
			List<JedisShardInfo> shards, Hashing algo) {
		this(poolConfig, shards, algo, null);
	}

	public ShardedJedisPool(final GenericObjectPool.Config poolConfig,
			List<JedisShardInfo> shards, Pattern keyTagPattern) {
		this(poolConfig, shards, Hashing.MURMUR_HASH, keyTagPattern);
	}

	public ShardedJedisPool(final GenericObjectPool.Config poolConfig,
			List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
		super(poolConfig, new ShardedJedisFactory(shards, algo, keyTagPattern));
//		this.startFailMonitor();
		
	}

	/**
	 * PoolableObjectFactory custom impl.
	 */
	private static class ShardedJedisFactory extends BasePoolableObjectFactory {
		private List<JedisShardInfo> shards;
		private Hashing algo;
		private Pattern keyTagPattern;

		public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo,
				Pattern keyTagPattern) {
			this.shards = shards;
			this.algo = algo;
			this.keyTagPattern = keyTagPattern;
		}

		public List<JedisShardInfo> getShards() {
			return shards;
		}

		public Object makeObject() throws Exception {
			ShardedJedis jedis = new ShardedJedis(shards, algo, keyTagPattern);
			return jedis;
		}

		public void destroyObject(final Object obj) throws Exception {
			if ((obj != null) && (obj instanceof ShardedJedis)) {
				ShardedJedis shardedJedis = (ShardedJedis) obj;
				for (Jedis jedis : shardedJedis.getAllShards()) {
					try {
						try {
							jedis.quit();
						} catch (Exception e) {

						}
						jedis.disconnect();
					} catch (Exception e) {

					}
				}
			}
		}

		public boolean validateObject(final Object obj) {
/*			try {
				ShardedJedis jedis = (ShardedJedis) obj;
				Collection<Jedis> jedisList = jedis.getAllShards();
				if(jedisList != null && shards!=null){
					if (jedisList.size() != this.shards.size()) {
						return false;
					}
				}
				return true;
			} catch (Exception ex) {
				return false;
			}*/
			try {
				ShardedJedis jedis = (ShardedJedis) obj;
				List<JedisShardInfo> shardInfos = jedis.getShardInfos();
//				boolean flag;
				if(shardInfos != null && shards!=null){
					if(shards.size() != shardInfos.size()) {
						return false;
					}
					for (int j = 0; j < shards.size(); j++) {
						JedisShardInfo jsInfo = shards.get(j);
						//验证顺序。
						JedisShardInfo currentjsInfo = shards.get(j);
						if (jsInfo.getHost().equals(currentjsInfo.getHost()) &&
								jsInfo.getPort()==currentjsInfo.getPort() && jsInfo.isAlive()==currentjsInfo.isAlive()) {
							continue;
						} else {
							return false;
						}
					}
				}
				return true;
			} catch (Exception ex) {
				
				return false;
			}
		}
	}

	// 添加的代码/////////////////////////////////////////////////////////
	
//	JedisFailMonitor failMonitor;
//
//	protected void startFailMonitor() {
//		System.out.println("线程启动");
//		if (failMonitor != null) {
//			if (failMonitor.isRunning()) {
//			} else {
//				failMonitor.start();
//			}
//		} else {
//			failMonitor = new JedisFailMonitor(this);
//			failMonitor.setInterval(3000);
//			failMonitor.start();
//		}
//	}
//
//	protected void stopFailMonitor() {
//		if (failMonitor != null && failMonitor.isRunning())
//			failMonitor.stopMonitor();
//	}

	
//定义线程类
//	protected static class JedisFailMonitor extends Thread {
//
//		private static final int DEFAULT_INTERVAL = 3 * 1000;
//	
//		private ShardedJedisPool shardPool;
//		private long interval = DEFAULT_INTERVAL; // every 3 seconds]
//		private boolean stopMonitor = false;
//		private boolean running;
//
//		public JedisFailMonitor(ShardedJedisPool shardPool, long interval) {
//			super();
//			this.shardPool = shardPool;
//			this.interval = interval;
//		}
//
//		public JedisFailMonitor(ShardedJedisPool shardPool) {
//			super();
//			this.shardPool = shardPool;
//		}
//
//		public void stopMonitor() {
//			this.stopMonitor = true;
//		}
//
//		@Override
//		public void run() {
//			this.running = true;
//			while (!this.stopMonitor) {
//				try {
//					Thread.sleep(interval);
//					shardPool.selfMonitor();
//					shardPool.selfEject();
//					shardPool.selfResume();
//				} catch (Exception e) {
//					System.out.println(e);
//					break;
//				}
//			}
//			this.running = false;
//		}
//
//		public long getInterval() {
//			return interval;
//		}
//
//		public void setInterval(long interval) {
//			this.interval = interval;
//		}
//
//		public boolean isStopMonitor() {
//			return stopMonitor;
//		}
//
//		public void setStopMonitor(boolean stopMonitor) {
//			this.stopMonitor = stopMonitor;
//		}
//
//		public boolean isRunning() {
//			return running;
//		}
//
//		public void setRunning(boolean running) {
//			this.running = running;
//		}
//
//	}

	public void selfMonitor() {
		ShardedJedis shardedJedis = getResource();
		if (shardedJedis == null) {
			return;
		}
		for (Iterator<Jedis> it = shardedJedis.getAllShards().iterator(); it
				.hasNext();) {
			Jedis jedis = it.next();
			try {
				jedis.ping();
			} catch (Exception e) {
				System.out.println("ping error" + e.getMessage());

				if (suspectJedises == null) {
					suspectJedises = new HashMap<String, JedisStatus>();
				}
				String hostAndPort = getHostAndPort(jedis);
				if (!suspectJedises.containsKey(hostAndPort)) {
					JedisStatus jedisStatus = new JedisStatus();
					suspectJedises.put(hostAndPort, jedisStatus);
				} else {
					JedisStatus jedisStatus = suspectJedises.get(hostAndPort);
					long failDuration = System.currentTimeMillis()
							- jedisStatus.getFirstFailTime();
					jedisStatus.setFailDuration(failDuration);
					jedisStatus.pingErrorCount++;
				}
				continue;
			}
		}
		returnResource(shardedJedis);
	}

	private static final long EJECT_ERROR_IN_TIME = 10 * 1000;//10S之内
	private static final int EJECT_ERROR_COUNT = 3;//3次连续ping不通
	
	public void selfEject() {
		if (suspectJedises != null && suspectJedises.size() > 0) {
			ArrayList<JedisShardInfo> shards = getCurrentShards();
			if (shards == null) {
				return;
			}
			for (Iterator<Entry<String, JedisStatus>> it = suspectJedises
					.entrySet().iterator(); it.hasNext();) {

				Entry<String, JedisStatus> entry = it.next();
				String hostAndPort = entry.getKey();
				JedisStatus jedisStatus = entry.getValue();
				
				if (jedisStatus.getFailDuration() > EJECT_ERROR_IN_TIME
						&& jedisStatus.pingErrorCount >= EJECT_ERROR_COUNT) {

					for (Iterator<JedisShardInfo> shardIt = shards.iterator(); shardIt
							.hasNext();) {
						JedisShardInfo shard = shardIt.next();
						if (getHostAndPort(shard).equals(hostAndPort) && shard.isAlive()) {
//							shardIt.remove();
							shard.setAlive(false);
							System.out.println("已剔除主机：" + hostAndPort);
							if(!ejectedShards.containsKey(hostAndPort)) {
								ejectedShards.put(hostAndPort, shard);
							}
							break;
						}
					}
				}
			}
		}
	}

	private ArrayList<JedisShardInfo> getCurrentShards() {
		ArrayList<JedisShardInfo> shards = null;
		if (factory != null && factory instanceof ShardedJedisFactory) {
			shards = (ArrayList<JedisShardInfo>) ((ShardedJedisFactory) factory)
					.getShards();
		}
		return shards;
	}
	
	public void selfResume() {
		if (ejectedShards != null && ejectedShards.size() > 0) {
			ArrayList<JedisShardInfo> shards = getCurrentShards();
			if (shards == null) {
				return;
			}
			boolean ifAnyResume = false;
			for (Iterator<Entry<String, JedisShardInfo>> it = ejectedShards
					.entrySet().iterator(); it.hasNext();) {
				Entry<String, JedisShardInfo> entry = it.next();
				String hostAndPort = entry.getKey();
				JedisShardInfo shard = entry.getValue();
				try {
					shard.createResource().ping();
					ifAnyResume = true;
					System.out.println("主机恢复" + hostAndPort);
					shard.setAlive(true);
//					shards.add(shard);
					it.remove();
					suspectJedises.remove(hostAndPort);
				} catch (Exception e) {
					continue;
				}
			}

			if (ifAnyResume) {
				System.out.println("清空池中闲置连接");
				clear();
			}
		}
	}
	
	
	private String getHostAndPort(Jedis jedis){
		try {
			return jedis.client.getHost() + ":" + jedis.client.getPort();
		} catch (Exception e) {
			return "";
		}
	}
	private String getHostAndPort(JedisShardInfo shard){
		try {
			return shard.getHost() + ":" + shard.getPort();
		} catch (Exception e) {
			return "";
		}
	}
	
	private Map<String,JedisStatus> suspectJedises;//可疑的主机
	private Map<String,JedisShardInfo> ejectedShards = new HashMap<String, JedisShardInfo>();//已经剔除的主机
	
	
//		if (factory != null) {
//			if (factory instanceof ShardedJedisFactory) {
//				ArrayList<JedisShardInfo> shards = (ArrayList<JedisShardInfo>) ((ShardedJedisFactory) factory)
//						.getShards();
//
//				for (Iterator<JedisShardInfo> it = shards.iterator(); it
//						.hasNext();) {
//					JedisShardInfo shard = it.next();
//					Jedis jedis = shard.createResource();
//					try {
//						String result = jedis.ping();
//					} catch (Exception e) {
//						System.out.println("ping error" + e.getMessage());
//						it.remove();
//						this.returnBrokenResource(getResource());
//						continue;
//					}
//				}
//
//			}
//		}
//	}

	private static class JedisStatus {
		private long firstFailTime;
		private long failDuration;
		public int pingErrorCount;

		public JedisStatus() {
			this.firstFailTime = System.currentTimeMillis();
			this.failDuration = 0;
			this.pingErrorCount = 1;
		}

		public long getFirstFailTime() {
			return firstFailTime;
		}

		public long getFailDuration() {
			return failDuration;
		}

		public void setFailDuration(long failDuration) {
			this.failDuration = failDuration;
		}

	}

}