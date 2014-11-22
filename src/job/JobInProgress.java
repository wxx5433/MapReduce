package job;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import jobtracker.JobTracker;
import nameNode.NameNodeService;
import node.NodeID;
import task.MapOutput;
import task.MapTask;
import task.ReduceTask;
import task.TaskAttemptID;
import task.TaskInProgress;
import configuration.Configuration;
import dfs.Service;
import fileSplit.FileSplit;

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

	// static final Log LOG = LogFactory.getLog(JobInProgress.class);
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

	final JobTracker jobTracker;

	// NetworkTopology Node to the set of TIPs
	// Map<Node, List<TaskInProgress>> nonRunningMapCache;

	// Map of NetworkTopology Node to set of running TIPs
	Map<NodeID, Map<Integer, TaskInProgress>> runningMapCache;

	// A set of non-local running maps
	Set<TaskInProgress> nonRunningMaps;

	// All failed map tasks
	Set<TaskInProgress> failedMaps;

	// A list of non-running reduce TIPs
	Set<TaskInProgress> nonRunningReduces;

	// All failed reduce tasks
	Set<TaskInProgress> failedReduces;

	// A set of running reduce TIPs
	Map<Integer, TaskInProgress> runningReduces;

	private volatile boolean tasksInit;
	private volatile boolean jobComplete;

	long startTime;
	long launchTime;
	long finishTime;

	// private MyConfiguration conf;
	private JobConf conf;
	private JobID jobId;

	private NameNodeService nameNodeService;
	private Configuration configuration;

	private static final FailComparator failComparator = new FailComparator();

	/**
	 * Create an almost empty JobInProgress, which can be used only for tests
	 */
	public JobInProgress(Configuration configuration, JobID jobid,
			JobConf conf, JobTracker tracker) throws IOException {
		this.configuration = configuration;
		this.tasksInit = false;
		this.jobComplete = false;
		this.conf = conf;
		this.jobId = jobid;
		// this.numMapTasks = conf.getNumMapTasks();
		this.numReduceTasks = conf.getNumReduceTasks();
		this.jobTracker = tracker;
		this.failedMaps = new TreeSet<TaskInProgress>(failComparator);
		this.failedReduces = new TreeSet<TaskInProgress>(failComparator);
		this.nonRunningMaps = new LinkedHashSet<TaskInProgress>();
		this.nonRunningReduces = new TreeSet<TaskInProgress>(failComparator);
		this.runningReduces = new ConcurrentHashMap<Integer, TaskInProgress>();
		this.nameNodeService = getNameNodeService();
		this.runningMapCache = new ConcurrentHashMap<NodeID, Map<Integer, TaskInProgress>>();
	}

	/**
	 * Construct the splits, etc. This is invoked from an async thread so that
	 * split-computation doesn't block anyone.
	 */
	public synchronized void initTasks() throws IOException {
		// return if the job has already initialize tasks or finished.
		if (tasksInit || isComplete()) {
			return;
		}

		System.out.println("Initializing " + jobId);
		// final long startTimeFinal = this.startTime;
		// log job info as the user running the job

		// read input splits and create a map per a split
		FileSplit[] splits = createSplits();
		numMapTasks = splits.length;
		System.out.println("Number of map tasks: " + numMapTasks);
		maps = new TaskInProgress[numMapTasks];
		for (int i = 0; i < numMapTasks; ++i) {
			maps[i] = new TaskInProgress(this.jobId, i, splits[i], true);
			nonRunningMaps.add(maps[i]);
		}
		System.out.println("Successfully initialized all map tasks for job: "
				+ jobId);
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

		// Create reduce tasks
		reduces = new TaskInProgress[numReduceTasks];
		for (int i = 0; i < numReduceTasks; i++) {
			// reduces[i] = new TaskInProgress(jobId, jobFile, numMapTasks, i,
			// jobtracker, conf, this, numSlotsPerReduce);
			reduces[i] = new TaskInProgress(this.jobId, i,
					false);
			nonRunningReduces.add(reduces[i]);
		}
		System.out
				.println("Successfully initialized all reduce tasks for job: "
						+ jobId);

		System.out.println("Job " + jobId + " initialized successfully with "
				+ numMapTasks + " map tasks and " + numReduceTasks
				+ " reduce tasks.");
	}

	/**
	 * get a new Map task
	 * 
	 * @return the index in tasks , -1 for no task
	 */
	public synchronized int getNewMapTask(final NodeID taskTrackerNodeID) {
		if (numMapTasks == 0) {
			System.out.println("No Map to schedule for " + jobId);
			return -1;
		}
		TaskInProgress tip = null;
		// first schedule a local fail map
		tip = findLocalTaskFromList(taskTrackerNodeID, failedMaps);
		// no local fail map, then try a non local fail map
		if (tip == null) {
			tip = findNonLocalTaskFromList(failedMaps);
		}
		if (tip != null) {
			scheduleMap(taskTrackerNodeID, tip);
			System.out.println("Choosing a failed map task ");
			// remove the map task from failedMaps
			failedMaps.remove(tip);
			// update attemp time
			tip.increaseTaskAttemptNum();
			// exceed max attempt time, the job fail!!!!!!!!!!!!!!
			if (tip.getTaskAttemptNum() > configuration.maxAttempsNum) {
				jobFailed = true;
			}
			return tip.getTIPId();
		}

		// no fail map, then schedule local non-running map tasks
		tip = findLocalTaskFromList(taskTrackerNodeID, nonRunningMaps);
		if (tip == null) {   // no local non-runnnig map tasks, arbitraly pick one.
			tip = findNonLocalTaskFromList(nonRunningMaps);
		}
		if (tip != null) {
			scheduleMap(taskTrackerNodeID, tip);
			System.out.println("Choosing a nonrunning map task");
			nonRunningMaps.remove(tip);
			return tip.getTIPId();
		}
		return -1;
	}

	/**
	 * return a non local task from the list
	 * @param tips
	 * @return
	 */
	public synchronized TaskInProgress findNonLocalTaskFromList(
			Collection<TaskInProgress> tips) {
		Iterator<TaskInProgress> iter = tips.iterator();
		if (iter.hasNext()) {
			return iter.next();
		}
		return null;
	}
	
	/**
	 * return a local task from the list
	 * 
	 * @param tips
	 * @return
	 */
	public synchronized TaskInProgress findLocalTaskFromList(
			NodeID taskTrackerNodeId, Collection<TaskInProgress> tips) {
		Iterator<TaskInProgress> iter = tips.iterator();
		TaskInProgress tip = null;
		while (iter.hasNext()) {
			 tip = iter.next();
			 if (isLocal(taskTrackerNodeId, tip)) {
				 return tip;
			 }
		}
		return null;
	}
	
	/**
	 * judge if the taskInProgress's input split is local to the taskTracker
	 * @param taskTrackerNodeId
	 * @param tip
	 * @return
	 */
	private synchronized boolean isLocal(NodeID taskTrackerNodeId, TaskInProgress tip) {
		List<String> locations = tip.getSplitLocations();
		for (String location: locations) {
			if (location.equals(taskTrackerNodeId.toString())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * get a new Reduce task
	 * 
	 * @return
	 */
	public synchronized int getNewReduceTask(NodeID taskTrackerNodeID) {
		if (numReduceTasks == 0) {
			System.out.println("No Map to schedule for " + jobId);
			return -1;
		}
		TaskInProgress tip = null;
		// first schedule a fail reduce
		tip = findNonLocalTaskFromList(failedReduces);
		if (tip != null) {
			scheduleReduce(tip);
			System.out.println("Choosing a failed reduce task ");
			failedReduces.remove(tip);
			return tip.getTIPId();
		}

		// then schedule non-running map tasks
		// TODO Auto-generated method stub
		// currently we do not consider locality
		// NodeID taskTrackerNodeId = tt.getNodeId();
		tip = findNonLocalTaskFromList(nonRunningReduces);
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
	public synchronized void scheduleMap(NodeID taskTrackerNodeId,
			TaskInProgress tip) {
		List<String> splitLocations = tip.getSplitLocations();
		// There may be some problems, if the task is a previously failed one
		// we add them back to nonRunningMaps
		if (splitLocations == null || splitLocations.size() == 0) {
			// add back to nonRunning map
			nonRunningMaps.add(tip);
			return;
		}

		Map<Integer, TaskInProgress> tasks = null;
		if (!runningMapCache.containsKey(taskTrackerNodeId)) {
			tasks = new ConcurrentHashMap<Integer, TaskInProgress>();
		} else {
			tasks = runningMapCache.get(taskTrackerNodeId);
		}
		tasks.put(tip.getTIPId(), tip);
		runningMapCache.put(taskTrackerNodeId, tasks);
		++runningMapTasks;
	}

	/**
	 * Schedule a reduce tasks
	 */
	public synchronized void scheduleReduce(TaskInProgress tip) {
		runningReduces.put(tip.getTIPId(), tip);
		++runningReduceTasks;
	}

	/**
	 * remove from running set and add to fail set
	 * 
	 * @param mapTask
	 */
	public synchronized void failMap(NodeID taskTrackerNodeID, MapTask mapTask) {
		if (runningMapCache.containsKey(taskTrackerNodeID)) {
			TaskInProgress tip = removeMapTaskInProgress(taskTrackerNodeID,
					mapTask);
//			runningMapCache.get(taskTrackerNodeID).remove(tip);
			failedMaps.add(tip);
			++this.failedMapTasks;
		}
	}

	/**
	 * remove a task in the runningMapCache
	 * @param taskTrackerNodeID
	 * @param mapTask
	 * @return
	 */
	public synchronized TaskInProgress removeMapTaskInProgress(
			NodeID taskTrackerNodeID, MapTask mapTask) {
		Map<Integer, TaskInProgress> runningMap = runningMapCache
				.get(taskTrackerNodeID);
		TaskInProgress result = null;
		int taskId = mapTask.getTaskAttemptID().getTaskID();
		if (runningMap.containsKey(taskId)) {
			runningMap.remove(taskId);
		}
		return result;
	}

	/**
	 * remove from running set and add to fail set
	 */
	public synchronized void failReduce(NodeID taskTrackerNodeID,
			ReduceTask reduceTask) {
		TaskInProgress tip = getReduceTaskInProgress(taskTrackerNodeID,
				reduceTask);
		runningReduces.remove(tip.getTIPId());
		failedReduces.add(tip);
		++this.failedReduceTasks;
	}

	public synchronized TaskInProgress getReduceTaskInProgress(
			NodeID taskTrackerNodeID, ReduceTask reduceTask) {
		TaskInProgress result = null;
		int taskId = reduceTask.getTaskAttemptID().getTaskID();
		if (runningReduces.containsKey(taskId)) {
			result = runningReduces.get(taskId);
		}
		runningReduces.remove(taskId);
		return result;
	}

	public void setJobCompelete() {
		this.jobComplete = true;
		jobTracker.jobComplete(this);
	}

	public boolean isComplete() {
		return this.jobComplete;
	}

	public void setJobKilled() {
		this.jobKilled = true;
		jobTracker.killJob(this.jobId);
	}

	public boolean isJobKilled() {
		return this.jobKilled;
	}

	/**
	 * Contact nameNode and get all the FileSplits
	 * 
	 * @return
	 */
	private FileSplit[] createSplits() {
		String inputPath = conf.getInputPath();
		try {
			return nameNodeService.getAllSplits(inputPath);
		} catch (RemoteException e) {
			System.out
					.println("Fail to get file splits from nameNode when initializing tasks");
			e.printStackTrace();
		}
		return null;
	}

	private NameNodeService getNameNodeService() {
		NodeID nameNodeId = new NodeID(configuration.nameNodeIP,
				configuration.nameNodePort);
		return Service.getNameNodeService(nameNodeId);
	}

	public synchronized void finishMapTask(MapTask mapTask) {
		System.out.println("Finish a map task: " + mapTask.toString());
		++this.finishedMapTasks;
		TaskAttemptID taskAttemptId = mapTask.getTaskAttemptID();
		System.out.println(taskAttemptId);
		// get all the tasks on the node where the finish map task is running on
		Map<Integer, TaskInProgress> tasks = runningMapCache.get(taskAttemptId
				.getNodeID());
		// remove it from runningMapCache
		tasks.remove(taskAttemptId.getTaskID());
		// add map's output to reducer's input
		List<String> mapOutputPathLists = mapTask.getOutputPathsForReduce();
		for (String data : mapOutputPathLists) {
			System.out.println("mapoutputPath: " + data);
		}
		for (int index = 0; index < mapOutputPathLists.size(); ++index) {
			MapOutput mapOutput = new MapOutput(taskAttemptId.getNodeID(),
					mapOutputPathLists.get(index));
			reduces[index].addMapOutput(mapOutput);
		}
	}

	public synchronized void finishReduceTask(ReduceTask reduceTask) {
		System.out.println("Finish a reduce task: " + reduceTask.toString());
		++this.finishedReduceTasks;
		TaskAttemptID tai = reduceTask.getTaskAttemptID();
		runningReduces.remove(tai.getTaskID());
		// finish the last reduce tasks
		if (this.finishedReduceTasks == this.numReduceTasks) {
			setJobCompelete();
		}
	}

	public boolean hasFinishedAllMapTasks() {
		return this.numMapTasks == this.finishedMapTasks;
	}

	public boolean isJobFailed() {
		return jobFailed;
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

	public JobConf getJobConf() {
		return this.conf;
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
			JobInProgress that = (JobInProgress) obj;
			if (this.jobId == that.jobId) {
				return true;
			}
		}
		return false;
	}

}

class FailComparator implements Comparator<TaskInProgress> {

	@Override
	public int compare(TaskInProgress o1, TaskInProgress o2) {
		return o1.getTaskAttemptNum() - o2.getTaskAttemptNum();
	}
}
