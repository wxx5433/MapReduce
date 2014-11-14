package partitioner;

public abstract class Partitioner<KEY, VALUE> {
	public abstract int getPartition(KEY key, VALUE value, int numPartitions);
}
