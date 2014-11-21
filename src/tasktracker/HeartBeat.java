package tasktracker;

import java.io.Serializable;
import java.util.ArrayList;

import node.NodeID;
import task.MapTask;
import task.ReduceTask;

public class HeartBeat implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -397894967836174469L;
	private NodeID nodeID;
	private int leftMapperSlot;
	private int leftReducerSlot;
	private ArrayList<MapTask> finishedMappers = new ArrayList<MapTask>();
	private ArrayList<ReduceTask> finishedReducers = new ArrayList<ReduceTask>();
	private ArrayList<MapTask> failedMappers = new ArrayList<MapTask>();
	private ArrayList<ReduceTask> failedReducers = new ArrayList<ReduceTask>();

	public HeartBeat(NodeID nodeID) {
		this.nodeID = nodeID;
	}

	public NodeID getNodeID() {
		return nodeID;
	}

	public void setNodeID(NodeID nodeID) {
		this.nodeID = nodeID;
	}

	public int getLeftMapperSlot() {
		return leftMapperSlot;
	}

	public void setLeftMapperSlot(int leftMapperSlot) {
		this.leftMapperSlot = leftMapperSlot;
	}

	public int getLeftReducerSlot() {
		return leftReducerSlot;
	}

	public void setLeftReducerSlot(int leftReducerSlot) {
		this.leftReducerSlot = leftReducerSlot;
	}

	public ArrayList<MapTask> getFinishedMappers() {
		return finishedMappers;
	}

	public void setFinishedMappers(ArrayList<MapTask> finishedMappers) {
		this.finishedMappers = finishedMappers;
	}

	public ArrayList<ReduceTask> getFinishedReducers() {
		return finishedReducers;
	}

	public void setFinishedReducers(ArrayList<ReduceTask> finishedReducers) {
		this.finishedReducers = finishedReducers;
	}

	public void addFinishedMapper(MapTask mapTask) {
		finishedMappers.add(mapTask);
	}

	public void addFinishedReducer(ReduceTask reduceTask) {
		finishedReducers.add(reduceTask);
	}

	public ArrayList<MapTask> getFailedMappers() {
		return failedMappers;
	}

	public void setFailedMappers(ArrayList<MapTask> failedMappers) {
		this.failedMappers = failedMappers;
	}

	public ArrayList<ReduceTask> getFailedReducers() {
		return failedReducers;
	}

	public void setFailedReducers(ArrayList<ReduceTask> failedReducers) {
		this.failedReducers = failedReducers;
	}

	public void addFailedMapper(MapTask mapTask) {
		failedMappers.add(mapTask);
	}

	public void addFailedReducer(ReduceTask reduceTask) {
		failedReducers.add(reduceTask);
	}

}
