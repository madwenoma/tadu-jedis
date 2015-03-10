# tadu-Jedis-failover
一个加入了简单failover逻辑的jedis包，构建于jedis 2.1.0版本。
#主要原理
- 参考memcahed java client的failover实现，主要原理是通过开启一个检测线程，定期通过redis的ping协议检测redis状态，如果发现有ping不通redis实例，则将该实例加入可疑列表，如果满足一定时间内连续N次ping不通的策略则视为当机，会在common pool连接池进行连接归还的时候validate失败
- 如果已当机的实例恢复，需销毁空闲连接，重新makeObject
- 保证了一致性哈希的特性，在每次创建新的redis连接池连接的时候，基于jedis的一致性哈希实现（TreeMap），会检测已创建的jedis实例的状态，如果发现有当机的redis实例，则会从哈希环中删除
#可能存在一致性问题
- 如果采用了一些有状态的key操作，如在实例A中给key01计数，当机后在实例B中计数就清零了
