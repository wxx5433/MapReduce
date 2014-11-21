package tasktracker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import job.JobClient;
import jobtracker.HeartBeatResponse;
import jobtracker.JobTrackerService;
import node.NodeID;
import task.MapTask;
import task.ReduceTask;
import task.Task;
import configuration.Configuration;

public class TaskTracker implements TaskTrackerInterface {
	private NodeID nodeId;
	JobTrackerService jobTrackerService;
	public BlockingDeque<MapTask> mapTaskQueue = new LinkedBlockingDeque<MapTask>();
	public BlockingDeque<ReduceTask> reduceTaskQueue = new LinkedBlockingDeque<ReduceTask>();
	private int mapperSlotNumber = 0;
	private int reducerSlotNumber = 0;

	private Thread mapperExecutor = null;
	private MapperExecutionExecutor mapperExecutionExecutor = null;
	private Thread reducerExecutor = null;
	private ReducerExecutionExecutor reducerExecutionExecutor = null;
	private Thread heartBeatThread = null;
	private HeartBeatThread heartBeat = null;

	private ArrayList<MapTask> completedMapTask = new ArrayList<MapTask>();
	private ArrayList<ReduceTask> completedReduceTask = new ArrayList<ReduceTask>();
	private ArrayList<MapTask> failedMapTask = new ArrayList<MapTask>();
	private ArrayList<ReduceTask> failedReduceTask = new ArrayList<ReduceTask>();

	public TaskTracker(Configuration conf) throws UnknownHostException {
		mapperSlotNumber = conf.mapSlots;
		reducerSlotNumber = conf.reduceSlots;
		nodeId = new NodeID(InetAddress.getLocalHost().getHostAddress(),
				conf.dataNodePort);
	}

	public void start() {
		jobTrackerService = JobClient.getJobTrackerService();
		heartBeat = new HeartBeatThread(this);
		heartBeatThread = new Thread(heartBeat);
		mapperExecutionExecutor = new MapperExecutionExecutor(this);
		reducerExecutionExecutor = new ReducerExecutionExecutor(this);
		mapperExecutor = new Thread(mapperExecutionExecutor);
		reducerExecutor = new Thread(reducerExecutionExecutor);
		heartBeatThread.start();
		mapperExecutor.start();
		reducerExecutor.start();
	}

	/**
	 * JobInProgress need node information to allocate tasks to dataNode
	 * contains the file split.
	 * 
	 * @return
	 */
	public NodeID getNodeId() {
		return this.nodeId;
	}

	public int getMapperSlotNumber() {
		return mapperSlotNumber;
	}

	public int getReducerSlotNumber() {
		return reducerSlotNumber;
	}

	public void sendHeartbeat() {
		HeartBeat heartBeat = new HeartBeat(this.nodeId)
				.setLeftMapperSlot(getLeftMapperSlotNum())
				.setLeftReducerSlot(getLeftMapperSlotNum())
				.setFinishedMappers(getCompletedMapTask())
				.setFinishedReducers(getCompletedReduceTask())
				.setFailedMappers(getFailedMapTask())
				.setFailedReducers(getFailedReduceTask());
		try {
			HeartBeatResponse heatBeatResponse = jobTrackerService
					.updateTaskTrackerStatus(heartBeat);
			clearTasks();
			if (!heatBeatResponse.getNewMappers().isEmpty()) {
				ArrayList<MapTask> newMappers = heatBeatResponse
						.getNewMappers();
				for (MapTask mapTask : newMappers) {
					addNewMapTask(mapTask);
				}
			}
			if (!heatBeatResponse.getNewReducers().isEmpty()) {
				ArrayList<ReduceTask> newReducers = heatBeatResponse
						.getNewReducers();
				for (ReduceTask reduceTask : newReducers) {
					addNewReduceTask(reduceTask);
				}
			}
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void clearTasks() {
		completedMapTask.clear();
		completedReduceTask.clear();
		failedMapTask.clear();
		failedReduceTask.clear();
	}

	public void addNewMapTask(MapTask mapTask) {
		mapTaskQueue.add(mapTask);
	}

	public void addNewReduceTask(ReduceTask reduceTask) {
		reduceTaskQueue.add(reduceTask);
	}

	public synchronized void updateCompletedTask(Task task) {
		if (task instanceof MapTask) {
			completedMapTask.add((MapTask) task);
		} else if (task instanceof ReduceTask) {
			completedReduceTask.add((ReduceTask) task);
		}
	}

	public synchronized void updateFailedTaskStatus(Task task) {
		if (task instanceof MapTask) {
			failedMapTask.add((MapTask) task);
		} else if (task instanceof ReduceTask) {
			failedReduceTask.add((ReduceTask) task);
		}
	}

	public int getLeftMapperSlotNum() {
		return mapperSlotNumber - mapperExecutionExecutor.getRunningThreadNum();
	}

	public int getLeftReducerSlotNum() {
		return reducerSlotNumber
				- reducerExecutionExecutor.getRunningThreadNum();
	}

	public ArrayList<MapTask> getCompletedMapTask() {
		return completedMapTask;
	}

	public ArrayList<ReduceTask> getCompletedReduceTask() {
		return completedReduceTask;
	}

	public ArrayList<MapTask> getFailedMapTask() {
		return failedMapTask;
	}

	public ArrayList<ReduceTask> getFailedReduceTask() {
		return failedReduceTask;
	}

	public static void main(String[] args) throws UnknownHostException {
		Configuration conf = new Configuration();
		TaskTracker taskTracker = new TaskTracker(conf);
		taskTracker.start();
	}

}
