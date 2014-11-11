package job;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jobtracker.JobTracker;
import node.Node;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import task.TaskInProgress;
import configuration.MyConfiguration;

public class JobInProgress {
	/**
	 * Used when the a kill is issued to a job which is initializing.
	 */
	@SuppressWarnings("serial")
	static class KillInterruptedException extends InterruptedException {
		public KillInterruptedException(String msg) {
			super(msg);
		}
	}

	static final Log LOG = LogFactory.getLog(JobInProgress.class);
	Job job = null;
	TaskInProgress maps[] = new TaskInProgress[0];
	TaskInProgress reduces[] = new TaskInProgress[0];
	int numMapTasks = 0;
	int numReduceTasks = 0;
	volatile int numSlotsPerMap = 1;
	volatile int numSlotsPerReduce = 1;

	// Counters to track currently running/finished/failed Map/Reduce
	// task-attempts
	int runningMapTasks = 0;
	int runningReduceTasks = 0;
	int finishedMapTasks = 0;
	int finishedReduceTasks = 0;
	int failedMapTasks = 0;
	int failedReduceTasks = 0;

	private volatile boolean jobKilled = false;
	private volatile boolean jobFailed = false;

	final JobTracker jobtracker;

	// NetworkTopology Node to the set of TIPs
	Map<Node, List<TaskInProgress>> nonRunningMapCache;

	// Map of NetworkTopology Node to set of running TIPs
	Map<Node, Set<TaskInProgress>> runningMapCache;

	// A set of non-local running maps
	Set<TaskInProgress> nonLocalRunningMaps;

	// A list of non-running reduce TIPs
	Set<TaskInProgress> nonRunningReduces;

	// A set of running reduce TIPs
	Set<TaskInProgress> runningReduces;

	// List<TaskCompletionEvent> taskCompletionEvents;

	long startTime;
	long launchTime;
	long finishTime;

	private MyConfiguration conf;
	private JobID jobId;

	/**
	 * Create an almost empty JobInProgress, which can be used only for tests
	 */
	protected JobInProgress(JobID jobid, MyConfiguration conf,
			JobTracker tracker) throws IOException {
		this.conf = conf;
		this.jobId = jobid;
		// this.numMapTasks = conf.getNumMapTasks();
		// this.numReduceTasks = conf.getNumReduceTasks();
		this.jobtracker = tracker;
		// this.failedMaps = new TreeSet<TaskInProgress>(failComparator);
		this.nonLocalRunningMaps = new LinkedHashSet<TaskInProgress>();
		// this.nonRunningReduces = new TreeSet<TaskInProgress>(failComparator);
		this.runningReduces = new LinkedHashSet<TaskInProgress>();
	}

	/**
	 * Construct the splits, etc. This is invoked from an async thread so that
	 * split-computation doesn't block anyone.
	 */
	public synchronized void initTasks() throws IOException,
			KillInterruptedException {
		// if (tasksInited || isComplete()) {
		// return;
		// }

		LOG.info("Initializing " + jobId);
		final long startTimeFinal = this.startTime;
		// log job info as the user running the job

		//
		// read input splits and create a map per a split
		//
		// TaskSplitMetaInfo[] splits = createSplits(jobId);
		// numMapTasks = splits.length;
		//
		// jobtracker.getInstrumentation().addWaitingMaps(getJobID(),
		// numMapTasks);
		// jobtracker.getInstrumentation().addWaitingReduces(getJobID(),
		// numReduceTasks);
		//
		// maps = new TaskInProgress[numMapTasks];
		// for (int i = 0; i < numMapTasks; ++i) {
		// inputLength += splits[i].getInputDataLength();
		// maps[i] = new TaskInProgress(jobId, jobFile, splits[i], jobtracker,
		// conf, this, i, numSlotsPerMap);
		// }
		// LOG.info("Input size for job " + jobId + " = " + inputLength
		// + ". Number of splits = " + splits.length);

		// set the launch time
		// this.launchTime = jobtracker.getClock().getTime();

		//
		// Create reduce tasks
		//
		this.reduces = new TaskInProgress[numReduceTasks];
		for (int i = 0; i < numReduceTasks; i++) {
			// reduces[i] = new TaskInProgress(jobId, jobFile, numMapTasks, i,
			// jobtracker, conf, this, numSlotsPerReduce);
			nonRunningReduces.add(reduces[i]);
		}

		// Log the number of map and reduce tasks
		LOG.info("Job " + jobId + " initialized successfully with "
				+ numMapTasks + " map tasks and " + numReduceTasks
				+ " reduce tasks.");
	}

}
