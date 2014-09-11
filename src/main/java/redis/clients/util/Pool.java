package redis.clients.util;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public abstract class Pool<T> {
    private final GenericObjectPool internalPool;
    protected PoolableObjectFactory factory;
    public Pool(final GenericObjectPool.Config poolConfig,
            PoolableObjectFactory factory) {
        this.internalPool = new GenericObjectPool(factory, poolConfig);
        this.factory = factory;
    }

    
    @SuppressWarnings("unchecked")
    public T getResource() {
        try {
        	T t = (T) internalPool.borrowObject();
            return t;
        } catch (Exception e) {
            throw new JedisConnectionException(
                    "Could not get a resource from the pool", e);
        }
    }
        
    public void returnResourceObject(final Object resource) {
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            throw new JedisException(
                    "Could not return the resource to the pool", e);
        }
    }
    
    public void returnBrokenResource(final T resource) {
    	returnBrokenResourceObject(resource);
    }
    
    public void returnResource(final T resource) {
    	returnResourceObject(resource);
    }

    protected void returnBrokenResourceObject(final Object resource) {
        try {
            internalPool.invalidateObject(resource);
        } catch (Exception e) {
            throw new JedisException(
                    "Could not return the resource to the pool", e);
        }
    }

    public void destroy() {
        try {
            internalPool.close();
        } catch (Exception e) {
            throw new JedisException("Could not destroy the pool", e);
        }
    }
    
    public void clear() {
    	try {
			internalPool.clear();
		} catch (Exception e) {
			throw new JedisException("Could not clear the pool", e);
		}
    }
    
    
}