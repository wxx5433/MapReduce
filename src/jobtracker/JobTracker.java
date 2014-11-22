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
import fileSplit.MapInputSplit;
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
	private int nodeIDCounter;

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

	//	private Map<TaskTracker, Long> taskTrackers;
	private Map<NodeID, Long> taskTrackers;

	private Configuration configuration;

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
		configuration = new Configuration();
		//		taskTrackers = new ConcurrentHashMap<TaskTracker, Long>();
		taskTrackers = new ConcurrentHashMap<NodeID, Long>();
		jobIDCounter = 0;
		taskIDCounter = 0;
		nodeIDCounter = 0;
		jobQueue = new ConcurrentLinkedQueue<JobInProgress>();
		completeJobQueue = new ConcurrentLinkedQueue<JobInProgress>();
		jobMap = new ConcurrentHashMap<JobID, JobInProgress>();
		//		taskPriorityQueue = new TaskPriorityQueue(taskPriorityQueueInitCapacity);
	}

	private void initialize() {
		// get nameNodeService
		NodeID nameNodeID = new NodeID(configuration.nameNodeIP, configuration.nameNodePort);
		nameNodeService = Service.getNameNodeService(nameNodeID);

		NodeID jobTrackerNodeID = new NodeID(configuration.jobTrackerIP, configuration.jobTrackerPort);
		// launch jobTracker service
		offerService(jobTrackerNodeID);
	}

	/**
	 * launch JobTrackerService
	 * @param jobTrackerNodeID
	 */
	private void offerService(NodeID jobTrackerNodeID) {
		jobTrackerService = new JobTrackerServiceImpl(this);
		String name = "rmi://" + jobTrackerNodeID.toString() + "/JobTrackerService";
		JobTrackerService stub;
		try {
			stub = (JobTrackerService) UnicastRemoteObject.exportObject(jobTrackerService, 0);
			try {
				Registry registry = LocateRegistry.getRegistry();
				registry.rebind(name, stub);
			} catch (Exception e) {
				Registry registry = LocateRegistry.createRegistry(1099);
				registry.rebind(name, stub);
			}
			System.out.println("Job tracker start service!!");
		} catch (RemoteException e) {
			System.out.println("Fail to export JobTrackerService");
			e.printStackTrace();
		}
	}

	public synchronized void addTaskTracker(NodeID taskTrackerNodeID) {
		taskTrackers.put(taskTrackerNodeID, System.currentTimeMillis());
		System.out.println("New task tracker online: " + taskTrackerNodeID.toString());
	}

	/**
	 * Remove it if have not received heart beat for a long time
	 * @param taskTracker
	 */
	public synchronized void removeTaskTracker(NodeID taskTrackerNodeID) {
		taskTrackers.remove(taskTrackerNodeID);
		System.out.println("Task tracker offline: " + taskTrackerNodeID.toString());
	}

	/**
	 * schedule a map task to taskTracker
	 */
	// can only change to one call to JobInProgress
	public MapTask getNewMapTask(NodeID taskTrackerNodeID) {
		// allocate next job's map tasks until 
		// there is no map task in the current job
		System.out.println("Task tracker: " + taskTrackerNodeID + " request new map task");
		for (JobInProgress jip: jobQueue) {
			int taskId = jip.getNewMapTask(taskTrackerNodeID);
			if (taskId != -1) {
				TaskInProgress tip = jip.getMapTask(taskId);
				List<String> locations = tip.getSplitLocations();
				if (locations.size() == 0) {
					return null;
				}
				// randomly choose one location (no locality here)
				MapInputSplit mis = new MapInputSplit(tip.getFileSplit());
				System.out.println("Print in JobTracker -- JobConf: " + jip.getJobConf().getMapperClass());
				tip.getTaskAttemptID().setNodeID(mis.getDataNodeID());
				MapTask mapTask = new MapTask(mis, taskTrackerNodeID.getLocalPath(),
						jip.getJobConf(), tip.getTaskAttemptID());
				System.out.println("Task tracker: " + taskTrackerNodeID 
						+ " successfully get new map task");
				return mapTask;
			}
		}
		return null;
	}

	/**
	 * schedule a reduce task to taskTracker
	 * @return
	 */
	public ReduceTask getNewReduceTask(NodeID taskTrackerNodeID) {
		System.out.println("Task tracker: " + taskTrackerNodeID + " request new reduce task");
		for (JobInProgress jip: jobQueue) {
			// has not finish all map tasks for this job
			if (!jip.hasFinishedAllMapTasks()) {
				continue;
			}
			int taskId = jip.getNewReduceTask(taskTrackerNodeID);
			if (taskId != -1) {
				TaskInProgress tip = jip.getReduceTask(taskId);
				//
				ReduceTask reduceTask = new ReduceTask(tip.getMapOutputList(), 
						jip.getJobConf(), tip.getTaskAttemptID());
				System.out.println("Task tracker: " + taskTrackerNodeID 
						+ " successfully get new reduce task");
				return reduceTask;
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
	
	public boolean isJobFailed(JobID jobId) {
		JobInProgress jip = jobMap.get(jobId);
		return jip.isJobFailed();
	}
	
	public boolean isJobKilled(JobID jobId) {
		JobInProgress jip = jobMap.get(jobId);
		return jip.isJobKilled();
	}
	
	public synchronized void killJob(JobID jobId) {
		JobInProgress jip = jobMap.get(jobId);
		jobQueue.remove(jip);
	}

	public synchronized boolean addJob(JobID jobID, JobConf conf) {
		JobInProgress jip = null;
		try {
			System.out.println("In JobTracker-addJob: jobconf:  " + conf.getMapperClass());
			jip = new JobInProgress(configuration, jobID, conf, this);
			jip.initTasks();
		} catch (IOException e) {
			return false;
		}
		// add to job queue
		jobQueue.add(jip);
		jobMap.put(jobID, jip);
		System.out.println("New job: " + jip.toString() + " added!!!");
		return true;
	}

	public int getFinishedMapTasksNum(JobID jobID) {
		JobInProgress jip = jobMap.get(jobID);
		return jip.getNumFinishedMapTasks();
	}

	public int getFinishedReduceTasksNum(JobID jobID) {
		JobInProgress jip = jobMap.get(jobID);
		return jip.getNumFinishedReduceTasks();
	}

	public int getMapTasksNum(JobID jobID) {
		JobInProgress jip = jobMap.get(jobID);
		return jip.getNumMapTasks();
	}

	public int getReduceTasksNum(JobID jobID) {
		JobInProgress jip = jobMap.get(jobID);
		return jip.getNumReduceTasks();
	}

	public JobInProgress getJobInProgress(JobID jobID) {
		if (!jobMap.containsKey(jobID)) {
			return null;
		}
		return jobMap.get(jobID);
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
	public static void main(String[] args) {
		JobTracker jobTracker = new JobTracker();
		jobTracker.initialize();
	}


}
