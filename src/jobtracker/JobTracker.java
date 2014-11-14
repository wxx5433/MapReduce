package jobtracker;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import configuration.Configuration;
import dfs.Service;
import nameNode.NameNodeService;
import node.NodeID;
import task.MapTask;
import task.ReduceTask;
import job.Job;
import job.JobInProgress;


public class JobTracker {

	private int jobIDCounter;
	// job queues to run
	private Queue<Job> jobs;
	// The job chosen to run
	private JobInProgress jobInProgress;
	// where each map task is allocated to 
	private Map<MapTask, NodeID> mapTasks;
	private Map<ReduceTask, NodeID> reduceTasks;
	
	// file system service to get input splits
	private NameNodeService nameNodeService;

	public JobTracker() {
		jobs = new PriorityQueue<Job>();
		jobInProgress = null;
		mapTasks = new ConcurrentHashMap<MapTask, NodeID>();
		reduceTasks = new ConcurrentHashMap<ReduceTask, NodeID>();
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
		NodeID nameNodeID = new NodeID(Configuration.masterIP, Configuration.masterPort);
		nameNodeService = Service.getNameNodeService(nameNodeID);
	}

}
