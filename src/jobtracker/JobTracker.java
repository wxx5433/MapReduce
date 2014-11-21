package jobtracker;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import configuration.Configuration;
import dfs.Service;
import nameNode.NameNodeService;
import node.NodeID;
import task.MapTask;
import task.ReduceTask;
import task.Task;
import task.TaskInProgress;
import tasktracker.TaskTracker;
import job.Job;
import job.JobConf;
import job.JobID;
import job.JobInProgress;


public class JobTracker {

	private int jobIDCounter;
	private int taskIDCounter;
	
//	// where each map task is allocated to 
//	private Map<MapTask, NodeID> mapTasks;
//	private Map<ReduceTask, NodeID> reduceTasks;
//	
//	private Map<JobID, Integer> mapTasksNum;
//	private Map<JobID, Integer> reduceTasksNum;

//	// finish tasks
//	private Map<JobID, Integer> finishedMapTasksNum;
//	private Map<JobID, Integer> finishedReduceTasksNum;
//	
//	// failed tasks
//	private Map<JobID, List<MapTask>> failedMapTasks;
//	private Map<JobID, List<ReduceTask>> failedReduceTasks;
	
	private Map<JobID, JobInProgress> jobMap;
	
	// job queue
	private Queue<JobInProgress> jobQueue;
	private Queue<JobInProgress> completeJobQueue;

	// file system service to get input splits
	private NameNodeService nameNodeService;

	// jobTracker Service
	private JobTrackerService jobTrackerService;
	
	private Map<TaskTracker, Long> taskTrackers;
	
	private Configuration configuraion;
	
	// task priority queue. job FIFO, map priority > reduce priority
//	TaskPriorityQueue taskPriorityQueue;
//	private final int taskPriorityQueueInitCapacity = 20;

	public JobTracker() {
//		mapTasksNum = new ConcurrentHashMap<JobID, Integer>();
//		reduceTasksNum = new ConcurrentHashMap<JobID, Integer>();
//		mapTasks = new ConcurrentHashMap<MapTask, NodeID>();
//		reduceTasks = new ConcurrentHashMap<ReduceTask, NodeID>();
//		finishedMapTasksNum = new ConcurrentHashMap<JobID, Integer>();
//		finishedReduceTasksNum = new ConcurrentHashMap<JobID, Integer>();
//		failedMapTasks = new ConcurrentHashMap<JobID, List<MapTask>>();
//		failedReduceTasks = new ConcurrentHashMap<JobID, List<ReduceTask>>();
		configuraion = new Configuration();
		taskTrackers = new ConcurrentHashMap<TaskTracker, Long>();
		jobIDCounter = 0;
		taskIDCounter = 0;
		jobQueue = new ConcurrentLinkedQueue<JobInProgress>();
		completeJobQueue = new ConcurrentLinkedQueue<JobInProgress>();
		jobMap = new ConcurrentHashMap<JobID, JobInProgress>();
//		taskPriorityQueue = new TaskPriorityQueue(taskPriorityQueueInitCapacity);
		initialize();
	}

	private void initialize() {
		// get nameNodeService
		NodeID masterNodeID = new NodeID(configuraion.nameNodeIP, configuraion.nameNodePort);
		nameNodeService = Service.getNameNodeService(masterNodeID);

		// launch jobTracker service
		offerService(masterNodeID);
	}

	/**
	 * launch JobTrackerService
	 * @param masterNodeID
	 */
	private void offerService(NodeID masterNodeID) {
		jobTrackerService = new JobTrackerServiceImpl(this);
		String name = "rmi://" + masterNodeID.toString() + "/JobTrackerService";
		NameNodeService stub;
		try {
			stub = (NameNodeService) UnicastRemoteObject.exportObject(jobTrackerService, 0);
			Registry registry = LocateRegistry.createRegistry(1099);
			registry.rebind(name, stub);
		} catch (RemoteException e) {
			System.out.println("Fail to export JobTrackerService");
			e.printStackTrace();
		}
	}
	
	public void addTaskTracker(TaskTracker taskTracker) {
		taskTrackers.put(taskTracker, System.currentTimeMillis());
	}
	
	/**
	 * Remove it if have not received heart beat for a long time
	 * @param taskTracker
	 */
	public void removeTaskTracker(TaskTracker taskTracker) {
		
	}
	
	/**
	 * schedule a map task to taskTracker
	 */
	// can only change to one call to JobInProgress
	public TaskInProgress getNewMapTask(TaskTracker tt) {
		// allocate next job's map tasks until 
		// there is no map task in the current job
		for (JobInProgress jip: jobQueue) {
			int taskId = jip.getNewMapTask(tt);
			if (taskId != -1) {
				return jip.getMapTask(taskId);
			}
		}
		return null;
	}
	
	/**
	 * schedule a reduce task to taskTracker
	 * @return
	 */
	public TaskInProgress getNewReduceTask(TaskTracker tt) {
		for (JobInProgress jip: jobQueue) {
			int taskId = jip.getNewReduceTask(tt);
			if (taskId != -1) {
				return jip.getReduceTask(taskId);
			}
		}
		return null;
	}
	
//	public synchronized Task nextJobTask() {
//		return taskPriorityQueue.poll();
//	}
	
	public synchronized int getNewJobID() {
		return ++jobIDCounter;
	}

	public synchronized int getNewTaskID() {
		return ++taskIDCounter;
	}
	
	// move job into compelete queue
	public synchronized void jobComplete(JobInProgress jip) {
		jobQueue.remove(jip);
		completeJobQueue.add(jip);
	}
	
	public boolean isJobComplete(JobID jobId) {
		JobInProgress jip = jobMap.get(jobId);
		return jip.isComplete();
	}
	
	public synchronized boolean addJob(JobID jobID, JobConf conf) {
		JobInProgress jip = null;
		try {
			jip = new JobInProgress(configuraion, jobID, conf, this);
		} catch (IOException e) {
			return false;
		}
		// add to job queue
		jobQueue.add(jip);
		jobMap.put(jobID, jip);
		return true;
	}
	
	public int getFinishedMapTasksNum(JobID jobID) {
		JobInProgress jip = jobMap.get(jobID);
		return jip.getNumFinishedMapTasks();
	}
	
	public int getFinishedReduceTasksNum(JobID jobID) {
		JobInProgress jip = jobMap.get(jobID);
		return jip.getNumReduceTasks();
	}
	
	public int getMapTasksNum(JobID jobID) {
		JobInProgress jip = jobMap.get(jobID);
		return jip.getNumMapTasks();
	}
	
	public int getReduceTasksNum(JobID jobID) {
		JobInProgress jip = jobMap.get(jobID);
		return jip.getNumReduceTasks();
	}
	
//	
//	public synchronized void addFailedMapTask(JobID jobID, MapTask mapTask) {
//		List<MapTask> mapTasks = null;
//		if (!failedMapTasks.containsKey(jobID)) {
//			mapTasks = new ArrayList<MapTask>();
//		} else {
//			failedMapTasks.get(jobID);
//		}
//		mapTasks.add(mapTask);
//		failedMapTasks.put(jobID, mapTasks);
//	}
//	
//	public synchronized void addFailedReduceTask(JobID jobID, ReduceTask reduceTask) {
//		List<ReduceTask> reduceTasks = null;
//		if (!failedReduceTasks.containsKey(jobID)) {
//			reduceTasks = new ArrayList<ReduceTask>();
//		} else {
//			failedMapTasks.get(jobID);
//		}
//		reduceTasks.add(reduceTask);
//		failedReduceTasks.put(jobID, reduceTasks);
//	}
//
//	public Iterable<MapTask> getFailedMapTasks(JobID jobID) {
//		return new ArrayList<MapTask>(failedMapTasks.get(jobID));
//	}
//
//	public Iterable<ReduceTask> getFailedReduceTasks(JobID jobID) {
//		return new ArrayList<ReduceTask>(failedReduceTasks.get(jobID));
//	}
	
	
	
}
