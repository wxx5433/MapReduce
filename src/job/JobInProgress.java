package job;

import configuration.Configuration;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Iterator;
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
import task.TaskAttemptID;
import task.TaskInProgress;
import tasktracker.TaskTracker;

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
	Map<NodeID, Set<TaskInProgress>> runningMapCache;

	// A set of non-local running maps
	Set<TaskInProgress> nonRunningMaps;

	// All failed map tasks
	Set<TaskInProgress> failedMaps;

	// A list of non-running reduce TIPs
	Set<TaskInProgress> nonRunningReduces;
	
	// All failed reduce tasks
	Set<TaskInProgress> failedReduces;

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
	public JobInProgress(JobID jobid, JobConf conf,
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
			maps[i] = new TaskInProgress(this.jobId, i, splits[i], true);
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
			// need to pass in arguments here!!!!!!!! (e.g. i)
			reduces[i] = new TaskInProgress(this.jobId, this.numMapTasks + i, false);
			nonRunningReduces.add(reduces[i]);
		}
		System.out.println("Successfully initialized all reduce tasks for job: " + jobId);

		System.out.println("Job " + jobId + " initialized successfully with "
				+ numMapTasks + " map tasks and " + numReduceTasks
				+ " reduce tasks.");
	}

	/**
	 * get a new Map task
	 * @return the index in tasks , -1 for no task
	 */
	// TODO Auto-generated method stub
	// need more arguments here!!!
	public synchronized int getNewMapTask(final TaskTracker tt) {
		if (numMapTasks == 0) {
			System.out.println("No Map to schedule for " + jobId);
			return -1;
		}
		TaskInProgress tip = null;
		// first schedule a fail map
		tip = findTaskFromList(failedMaps);
		if (tip != null) {
			scheduleMap(tt.getNodeId(), tip);
			System.out.println("Choosing a failed map task ");
			// remove the map task from failedMaps
			failedMaps.remove(tip);
			tip.
			return tip.getTIPId();
		}
		
		// then schedule non-running map tasks
		// TODO Auto-generated method stub
		// currently we do not consider locality
//		NodeID taskTrackerNodeId = tt.getNodeId();
		tip = findTaskFromList(nonRunningMaps);
		if (tip != null) {
			scheduleMap(tt.getNodeId(), tip);
			System.out.println("Choosing a nonrunning map task");
			nonRunningMaps.remove(tip);
			return tip.getTIPId();
		}
		return -1;
	}
	
	/**
	 * return a task from the list
	 * @param tips
	 * @return
	 */
	public synchronized TaskInProgress findTaskFromList (
			Collection<TaskInProgress> tips) {
		Iterator<TaskInProgress> iter = tips.iterator();
		if (iter.hasNext()) {
			return iter.next();
		}
		return null;
	}

	/**
	 * get a new Reduce task
	 * @return
	 */
	public synchronized int getNewReduceTask(TaskTracker tt) {
		if (numReduceTasks == 0) {
			System.out.println("No Map to schedule for " + jobId);
			return -1;
		}
		TaskInProgress tip = null;
		// first schedule a fail reduce
		tip = findTaskFromList(failedReduces);
		if (tip != null) {
			scheduleReduce(tip);
			System.out.println("Choosing a failed reduce task ");
			failedReduces.remove(tip);
			return tip.getTIPId();
		}
		
		// then schedule non-running map tasks
		// TODO Auto-generated method stub
		// currently we do not consider locality
//		NodeID taskTrackerNodeId = tt.getNodeId();
		tip = findTaskFromList(nonRunningReduces);
		if (tip != null) {
			scheduleReduce(tip);
			System.out.println("Choosing a nonrunning reduce task");
			nonRunningReduces.remove(tip);
			return tip.getTIPId();
		}
		return -1;
	}

	public TaskInProgress getMapTask(int taskId) {
		return maps[taskId];
	}
	
	public TaskInProgress getReduceTask(int taskId) {
		return reduces[taskId];
	}
	
	/**
	 * add a tip to a taskTracker
	 */
	public synchronized void scheduleMap(NodeID taskTrackerNodeId, TaskInProgress tip) {
		List<String> splitLocations = tip.getSplitLocations();
		// There may be some problems, if the task is a previously failed one
		// we add them back to nonRunningMaps
		if (splitLocations == null || splitLocations.size() == 0) {
			// add back to nonRunning map
			nonRunningMaps.add(tip);
			return;
		}
		
		Set<TaskInProgress> tasks = null;
		if (!runningMapCache.containsKey(taskTrackerNodeId)) {
			tasks = new LinkedHashSet<TaskInProgress>();
		} else {
			tasks = runningMapCache.get(taskTrackerNodeId);
		}
		tasks.add(tip);
		runningMapCache.put(taskTrackerNodeId, tasks);
		++runningMapTasks;
	}

	/**
	 * Schedule a reduce tasks
	 */
	public synchronized void scheduleReduce(TaskInProgress tip) {
		runningReduces.add(tip);
		++runningReduceTasks;
	}

	/**
	 * remove from running set and add to fail set
	 * @param mapTask
	 */
	public synchronized void failMap(TaskTracker tt, TaskInProgress tip) {
		NodeID taskTrackerNodeID = tt.getNodeId();
		if (runningMapCache.containsKey(taskTrackerNodeID)) {
			runningMapCache.get(taskTrackerNodeID).remove(tip);
		}
		failedMaps.add(tip);
		++this.failedMapTasks;
	}

	/**
	 * remove from running set and add to fail set
	 */
	public synchronized void failReduce(TaskTracker tt, TaskInProgress tip) {
		NodeID taskTrackerNodeID = tt.getNodeId();
		runningReduces.remove(tip);
		failedReduces.add(tip);
		++this.failedReduceTasks;
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
	
	public synchronized void finishMapTask(TaskAttemptID id) {
		++this.finishedMapTasks;
		// get all the tasks on the node where the finish map task is running on
		Set<TaskInProgress> tasks = runningMapCache.get(id.getNodeID());
		// remove it
		tasks.remove(maps[id.getTaskID()]);
	}
	
	public synchronized void finishReduceTask(TaskAttemptID id) {
		++this.finishedReduceTasks;
		runningReduces.remove(reduces[id.getTaskID() - this.numMapTasks]);
	}
	
	public int getNumMapTasks() {
		return this.numMapTasks;
	}
	
	public int getNumReduceTasks() {
		return this.numReduceTasks;
	}
	
	public int getNumFinishedMapTasks() {
		return this.finishedMapTasks;
	}
	
	public int getNumFinishedReduceTasks() {
		return this.finishedReduceTasks;
	}
	
	@Override
	public int hashCode() {
		return jobId.toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof JobInProgress) {
			JobInProgress that = (JobInProgress)obj;
			if (this.jobId == that.jobId) {
				return true;
			}
		}
		return false;
	}

}
