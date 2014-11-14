package partitioner;

public class HashPartitioner<K, V> extends Partitioner<K, V> {

	@Override
	public int getPartition(K key, V value, int numReduceTasks) {
		return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}

}
