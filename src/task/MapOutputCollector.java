package task;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import job.JobConf;

public interface MapOutputCollector<K, V> {
	public void init(CollectorContext context) throws FileNotFoundException;

	public void collect(K key, V value, int partition) throws IOException;

	public ArrayList<String> getOutputPaths();

	public void close() throws IOException;

	public static class CollectorContext {
		private final MapTask mapTask;
		private final JobConf jobConf;

		public CollectorContext(MapTask mapTask, JobConf jobConf) {
			this.mapTask = mapTask;
			this.jobConf = jobConf;
		}

		public MapTask getMapTask() {
			return mapTask;
		}

		public JobConf getJobConf() {
			return jobConf;
		}

	}
}
