package redis.clients.util;

public abstract class ShardInfo<T> {
    private int weight;
    private boolean isAlive;//添加分片状态信息，默认为true by -lf
    public boolean isAlive() {
		return isAlive;
	}
	public void setAlive(boolean isAlive) {
		this.isAlive = isAlive;
	}

	public ShardInfo() {
    }

    public ShardInfo(int weight) {
        this.weight = weight;
        this.isAlive = true; //添加分片状态信息，默认为true by -lf
    }
    
    public int getWeight() {
        return this.weight;
    }

    protected abstract T createResource();
    
    public abstract String getName();
}
