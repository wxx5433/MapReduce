package job;

import configuration.Configuration;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import configuration.MyConfiguration;
import dfs.Service;
import fileSplit.FileSplit;
import jobtracker.JobTracker;
import nameNode.NameNodeService;
import node.Node;
import node.NodeID;
import task.TaskInProgress;

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

	//	static final Log LOG = LogFactory.getLog(JobInProgress.class);
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
	//	Map<Node, List<TaskInProgress>> nonRunningMapCache;

	// Map of NetworkTopology Node to set of running TIPs
	Map<Node, Set<TaskInProgress>> runningMapCache;

	// A set of non-local running maps
	Set<TaskInProgress> nonRunningMaps;

	// All failed map tasks
	Set<TaskInProgress> failedMaps;

	// A list of non-running reduce TIPs
	Set<TaskInProgress> nonRunningReduces;

	// A set of running reduce TIPs
	Set<TaskInProgress> runningReduces;


	private volatile boolean tasksInit;
	private volatile boolean jobComplete;

	long startTime;
	long launchTime;
	long finishTime;

	//	private MyConfiguration conf;
	private JobConf conf;
	private JobID jobId;

	private NameNodeService nameNodeService;

	/**
	 * Create an almost empty JobInProgress, which can be used only for tests
	 */
	protected JobInProgress(JobID jobid, JobConf conf,
			JobTracker tracker) throws IOException {
		this.tasksInit= false;
		this.jobComplete = false;
		this.conf = conf;
		this.jobId = jobid;
		// this.numMapTasks = conf.getNumMapTasks();
		this.numReduceTasks = conf.getNumReduceTasks();
		this.jobtracker = tracker;
		this.failedMaps = new TreeSet<TaskInProgress>();//failComparator);
		this.nonRunningMaps = new LinkedHashSet<TaskInProgress>();
		this.nonRunningReduces = new TreeSet<TaskInProgress>();//failComparator);
		this.runningReduces = new LinkedHashSet<TaskInProgress>();
		this.nameNodeService = getNameNodeService();
	}

	/**
	 * Construct the splits, etc. This is invoked from an async thread so that
	 * split-computation doesn't block anyone.
	 */
	public synchronized void initTasks() throws IOException,
	KillInterruptedException {
		// return if the job has already initialize tasks or finished. 
		if (tasksInit || isComplete()) {
			return;
		}


		System.out.println("Initializing " + jobId);
		//		final long startTimeFinal = this.startTime;
		// log job info as the user running the job

		// read input splits and create a map per a split
		FileSplit[] splits = createSplits();
		numMapTasks = splits.length;
		maps = new TaskInProgress[numMapTasks];
		for (int i = 0; i < numMapTasks; ++i) {
			// TODO Auto-generated method stub
			// need to pass arguments here!!!!!!!!!!!!!!
			maps[i] = new TaskInProgress();
			nonRunningMaps.add(maps[i]);
		}
		System.out.println("Successfully initialized all map tasks for job: " + jobId);
		//
		// jobtracker.getInstrumentation().addWaitingMaps(getJobID(),
		// numMapTasks);
		// jobtracker.getInstrumentation().addWaitingReduces(getJobID(),
		// numReduceTasks);
		//
		//		 maps = new TaskInProgress[numMapTasks];
		// for (int i = 0; i < numMapTasks; ++i) {
		// inputLength += splits[i].getInputDataLength();
		// maps[i] = new TaskInProgress(jobId, jobFile, splits[i], jobtracker,
		// conf, this, i, numSlotsPerMap);
		// }
		// LOG.info("Input size for job " + jobId + " = " + inputLength
		// + ". Number of splits = " + splits.length);

		// set the launch time
		// this.launchTime = jobtracker.getClock().getTime();

		// Create reduce tasks
		reduces = new TaskInProgress[numReduceTasks];
		for (int i = 0; i < numReduceTasks; i++) {
			// reduces[i] = new TaskInProgress(jobId, jobFile, numMapTasks, i,
			// jobtracker, conf, this, numSlotsPerReduce);
			// TODO Auto-generated method stub
			// need to pass in arguments here!!!!!!!!
			reduces[i] = new TaskInProgress();
			nonRunningReduces.add(reduces[i]);
		}
		System.out.println("Successfully initialized all reduce tasks for job: " + jobId);

		System.out.println("Job " + jobId + " initialized successfully with "
				+ numMapTasks + " map tasks and " + numReduceTasks
				+ " reduce tasks.");
	}

	/**
	 * get a new Map task
	 */
	public TaskInProgress getNewMap() {

	}

	/**
	 * get a new Reduce task
	 * @return
	 */
	public TaskInProgress getNewReduce() {

	}

	/**
	 * Schedule a map task
	 */
	public void scheduleMap() {

	}

	/**
	 * Schedule a reduce tasks
	 */
	public void scheduleReduce() {

	}

	/**
	 * add to fail Map map
	 * @param mapTask
	 */
	public synchronized void failMap(TaskInProgress mapTask) {

	}

	/**
	 * add to fail Reduce map
	 */
	public synchronized void failReduce() {

	}

	public void setJobCompelete() {
		this.jobComplete = true;
	}

	public boolean isComplete() {
		return this.jobComplete;
	}

	/**
	 * Contact nameNode and get all the FileSplits
	 * @return
	 */
	private FileSplit[] createSplits() {
		String inputPath = conf.getInputPath();
		try {
			return nameNodeService.getAllSplits(inputPath);
		} catch (RemoteException e) {
			System.out.println("Fail to get file splits from nameNode when initializing tasks");
			e.printStackTrace();
		}
		return null;
	}

	private NameNodeService getNameNodeService() {
		NodeID nameNodeId = new NodeID(Configuration.masterIP, Configuration.masterPort);
		return Service.getNameNodeService(nameNodeId);
	}

}
