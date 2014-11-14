package task;

import java.io.IOException;
import java.util.ArrayList;

import job.Job;
import job.JobConf;
import jobtracker.JobTracker;

public class ReduceTask implements Task {
	private ArrayList<String> inputFiles;
	private String outputFileName;
	private Job job;
	private int taskID;
	private String phase;
	private int reducerID;

	public ReduceTask() {
	}

	@Override
	@SuppressWarnings("unchecked")
	public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
			throws IOException, InterruptedException, ClassNotFoundException {

		initialize(job, getJobID(), reporter, useNewApi);

		sslShuffle = job.getBoolean(JobTracker.SHUFFLE_SSL_ENABLED_KEY,
				JobTracker.SHUFFLE_SSL_ENABLED_DEFAULT);
		if (sslShuffle && sslFactory == null) {
			sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, job);
			try {
				sslFactory.init();
			} catch (Exception ex) {
				sslFactory.destroy();
				throw new RuntimeException(ex);
			}
		}

		// Initialize the codec
		codec = initCodec();

		boolean isLocal = "local"
				.equals(job.get("mapred.job.tracker", "local"));
		if (!isLocal) {
			ShuffleConsumerPlugin.Context context = new ShuffleConsumerPlugin.Context(
					umbilical, job, reporter, this);
			shuffleConsumerPlugin = ReflectionUtils.newInstance(job.getClass(
					JobContext.SHUFFLE_CONSUMER_PLUGIN_ATTR,
					ReduceCopier.class, ShuffleConsumerPlugin.class), job);
			LOG.info("Using ShuffleConsumerPlugin: "
					+ shuffleConsumerPlugin.getClass().getName());
			shuffleConsumerPlugin.init(context);
			if (!shuffleConsumerPlugin.fetchOutputs()) {
				if (shuffleConsumerPlugin.getMergeThrowable() instanceof FSError) {
					throw (FSError) shuffleConsumerPlugin.getMergeThrowable();
				}
				throw new IOException("Task: " + getTaskID()
						+ " - The shuffle consumer failed",
						shuffleConsumerPlugin.getMergeThrowable());
			}
		}
		copyPhase.complete(); // copy is already complete
		setPhase(TaskStatus.Phase.SORT);
		statusUpdate(umbilical);

		final FileSystem rfs = FileSystem.getLocal(job).getRaw();
		RawKeyValueIterator rIter = isLocal ? Merger.merge(job, rfs, job
				.getMapOutputKeyClass(), job.getMapOutputValueClass(), codec,
				getMapFiles(rfs, true), !conf.getKeepFailedTaskFiles(), job
						.getInt("io.sort.factor", 100), new Path(getTaskID()
						.toString()), job.getOutputKeyComparator(), reporter,
				spilledRecordsCounter, null) : shuffleConsumerPlugin
				.createKVIterator();

		// free up the data structures
		mapOutputFilesOnDisk.clear();

		sortPhase.complete(); // sort is complete
		setPhase(TaskStatus.Phase.REDUCE);
		statusUpdate(umbilical);
		Class keyClass = job.getMapOutputKeyClass();
		Class valueClass = job.getMapOutputValueClass();
		RawComparator comparator = job.getOutputValueGroupingComparator();

		if (useNewApi) {
			runNewReducer(job, umbilical, reporter, rIter, comparator,
					keyClass, valueClass);
		} else {
			runOldReducer(job, umbilical, reporter, rIter, comparator,
					keyClass, valueClass);
		}
		if (shuffleConsumerPlugin != null) {
			shuffleConsumerPlugin.close();
		}
		done(umbilical, reporter);

		if (sslFactory != null) {
			sslFactory.destroy();
		}
	}

	@SuppressWarnings("unchecked")
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runNewReducer(JobConf job,
			final TaskUmbilicalProtocol umbilical, final TaskReporter reporter,
			RawKeyValueIterator rIter, RawComparator<INKEY> comparator,
			Class<INKEY> keyClass, Class<INVALUE> valueClass)
			throws IOException, InterruptedException, ClassNotFoundException {
		// wrap value iterator to report progress.
		final RawKeyValueIterator rawIter = rIter;
		rIter = new RawKeyValueIterator() {
			public void close() throws IOException {
				rawIter.close();
			}

			public DataInputBuffer getKey() throws IOException {
				return rawIter.getKey();
			}

			public Progress getProgress() {
				return rawIter.getProgress();
			}

			public DataInputBuffer getValue() throws IOException {
				return rawIter.getValue();
			}

			public boolean next() throws IOException {
				boolean ret = rawIter.next();
				reducePhase.set(rawIter.getProgress().get());
				reporter.progress();
				return ret;
			}
		};
		// make a task context so we can get the classes
		org.apache.hadoop.mapreduce.TaskAttemptContext taskContext = new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(
				job, getTaskID(), reporter);
		// make a reducer
		org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = (org.apache.hadoop.mapreduce.Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>) ReflectionUtils
				.newInstance(taskContext.getReducerClass(), job);
		org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE> output = (org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE>) outputFormat
				.getRecordWriter(taskContext);
		org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE> trackedRW = new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(
				output, reduceOutputCounter);
		job.setBoolean("mapred.skip.on", isSkipping());
		org.apache.hadoop.mapreduce.Reducer.Context reducerContext = createReduceContext(
				reducer, job, getTaskID(), rIter, reduceInputKeyCounter,
				reduceInputValueCounter, trackedRW, committer, reporter,
				comparator, keyClass, valueClass);
		reducer.run(reducerContext);
		output.close(reducerContext);
	}

	public Task setInputFile(String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Task setOutputFile(String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Task setJob(Job job) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Task setTaskID(TaskAttemptID id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getOutputFile() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInputFile() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Job getJob() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TaskAttemptID getTaskID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void run(job.JobConf job) {
		// TODO Auto-generated method stub

	}
}
