package jobtracker;

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
import tasktracker.TaskTracker;
import job.Job;
import job.JobID;
import job.JobInProgress;


public class JobTracker {

	private int jobIDCounter;
	private int taskIDCounter;
	
	// job queues to run
	private Queue<Job> jobs;
	// The job chosen to run
	private Queue<JobInProgress> jobInProgress;
	// where each map task is allocated to 
	private Map<MapTask, NodeID> mapTasks;
	private Map<ReduceTask, NodeID> reduceTasks;
	
	private Map<JobID, Integer> mapTasksNum;
	private Map<JobID, Integer> reduceTasksNum;

	// finish tasks
	private Map<JobID, Integer> finishedMapTasksNum;
	private Map<JobID, Integer> finishedReduceTasksNum;
	
	// failed tasks
	private Map<JobID, List<MapTask>> failedMapTasks;
	private Map<JobID, List<ReduceTask>> failedReduceTasks;

	// file system service to get input splits
	private NameNodeService nameNodeService;

	// jobTracker Service
	private JobTrackerService jobTrackerService;
	
	private Map<TaskTracker, Long> taskTrackersHealth;
	
	// task priority queue. job FIFO, map priority > reduce priority
	TaskPriorityQueue taskPriorityQueue;
	private final int taskPriorityQueueInitCapacity = 20;

	public JobTracker() {
		jobs = new PriorityQueue<Job>();
		jobInProgress = new ConcurrentLinkedQueue<JobInProgress>();
		mapTasksNum = new ConcurrentHashMap<JobID, Integer>();
		reduceTasksNum = new ConcurrentHashMap<JobID, Integer>();
		mapTasks = new ConcurrentHashMap<MapTask, NodeID>();
		reduceTasks = new ConcurrentHashMap<ReduceTask, NodeID>();
		finishedMapTasksNum = new ConcurrentHashMap<JobID, Integer>();
		finishedReduceTasksNum = new ConcurrentHashMap<JobID, Integer>();
		failedMapTasks = new ConcurrentHashMap<JobID, List<MapTask>>();
		failedReduceTasks = new ConcurrentHashMap<JobID, List<ReduceTask>>();
		taskTrackersHealth = new ConcurrentHashMap<TaskTracker, Long>();
		jobIDCounter = 0;
		taskIDCounter = 0;
		taskPriorityQueue = new TaskPriorityQueue(taskPriorityQueueInitCapacity);
		initialize();
	}

	private void initialize() {
		try {
			Configuration.setup();
		} catch (Exception e) {
			System.out.println("Load configuration failed in JobTracker");
			e.printStackTrace();
		}
		// get nameNodeService
		NodeID masterNodeID = new NodeID(Configuration.masterIP, Configuration.masterPort);
		nameNodeService = Service.getNameNodeService(masterNodeID);

		// launch jobTracker service
		launchJobTrackerService(masterNodeID);
	}

	/**
	 * launch JobTrackerService
	 * @param masterNodeID
	 */
	private void launchJobTrackerService(NodeID masterNodeID) {
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
	
	public synchronized Task nextJobTask() {
		return taskPriorityQueue.poll();
	}
	
	public synchronized int getNewJobID() {
		return ++jobIDCounter;
	}

	public synchronized int getNewTaskID() {
		return ++taskIDCounter;
	}
	
	public int getFinishedMapTasksNum(JobID jobID) {
		return finishedMapTasksNum.get(jobID);
	}
	
	public int getFinishedReduceTasksNum(JobID jobID) {
		return finishedReduceTasksNum.get(jobID);
	}
	
	public int getMapTasksNum(JobID jobID) {
		return mapTasksNum.get(jobID);
	}
	
	public int getReduceTasksNum(JobID jobID) {
		return reduceTasksNum.get(jobID);
	}
	
	public synchronized void addFailedMapTask(JobID jobID, MapTask mapTask) {
		List<MapTask> mapTasks = null;
		if (!failedMapTasks.containsKey(jobID)) {
			mapTasks = new ArrayList<MapTask>();
		} else {
			failedMapTasks.get(jobID);
		}
		mapTasks.add(mapTask);
		failedMapTasks.put(jobID, mapTasks);
	}
	
	public synchronized void addFailedReduceTask(JobID jobID, ReduceTask reduceTask) {
		List<ReduceTask> reduceTasks = null;
		if (!failedReduceTasks.containsKey(jobID)) {
			reduceTasks = new ArrayList<ReduceTask>();
		} else {
			failedMapTasks.get(jobID);
		}
		reduceTasks.add(reduceTask);
		failedReduceTasks.put(jobID, reduceTasks);
	}

	public Iterable<MapTask> getFailedMapTasks(JobID jobID) {
		return new ArrayList<MapTask>(failedMapTasks.get(jobID));
	}

	public Iterable<ReduceTask> getFailedReduceTasks(JobID jobID) {
		return new ArrayList<ReduceTask>(failedReduceTasks.get(jobID));
	}
}
