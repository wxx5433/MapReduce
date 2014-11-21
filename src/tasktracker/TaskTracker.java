package tasktracker;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import node.NodeID;
import task.MapTask;
import task.ReduceTask;
import configuration.MyConfiguration;

public class TaskTracker implements TaskTrackerInterface {
	private NodeID nodeId;
	public BlockingDeque<MapTask> mapTaskQueue = new LinkedBlockingDeque<MapTask>();
	public BlockingDeque<ReduceTask> reduceTaskQueue = new LinkedBlockingDeque<ReduceTask>();
	private int mapperSlotNumber = 0;
	private int reducerSlotNumber = 0;

	public TaskTracker(MyConfiguration conf) {
		mapperSlotNumber = 4;
		reducerSlotNumber = 1;
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
		// TODO Auto-generated method stub
		return mapperSlotNumber;
	}

	public int getReducerSlotNumber() {
		// TODO Auto-generated method stub
		return mapperSlotNumber;
	}
}
