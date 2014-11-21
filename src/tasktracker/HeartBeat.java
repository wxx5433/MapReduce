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

	public HeartBeat setNodeID(NodeID nodeID) {
		this.nodeID = nodeID;
		return this;
	}

	public int getLeftMapperSlot() {
		return leftMapperSlot;
	}

	public HeartBeat setLeftMapperSlot(int leftMapperSlot) {
		this.leftMapperSlot = leftMapperSlot;
		return this;
	}

	public int getLeftReducerSlot() {
		return leftReducerSlot;
	}

	public HeartBeat setLeftReducerSlot(int leftReducerSlot) {
		this.leftReducerSlot = leftReducerSlot;
		return this;
	}

	public ArrayList<MapTask> getFinishedMappers() {
		return finishedMappers;
	}

	public HeartBeat setFinishedMappers(ArrayList<MapTask> finishedMappers) {
		this.finishedMappers = finishedMappers;
		return this;
	}

	public ArrayList<ReduceTask> getFinishedReducers() {
		return finishedReducers;
	}

	public HeartBeat setFinishedReducers(ArrayList<ReduceTask> finishedReducers) {
		this.finishedReducers = finishedReducers;
		return this;
	}

	public HeartBeat addFinishedMapper(MapTask mapTask) {
		finishedMappers.add(mapTask);
		return this;
	}

	public HeartBeat addFinishedReducer(ReduceTask reduceTask) {
		finishedReducers.add(reduceTask);
		return this;
	}

	public ArrayList<MapTask> getFailedMappers() {
		return failedMappers;
	}

	public HeartBeat setFailedMappers(ArrayList<MapTask> failedMappers) {
		this.failedMappers = failedMappers;
		return this;
	}

	public ArrayList<ReduceTask> getFailedReducers() {
		return failedReducers;
	}

	public HeartBeat setFailedReducers(ArrayList<ReduceTask> failedReducers) {
		this.failedReducers = failedReducers;
		return this;
	}

	public HeartBeat addFailedMapper(MapTask mapTask) {
		failedMappers.add(mapTask);
		return this;
	}

	public HeartBeat addFailedReducer(ReduceTask reduceTask) {
		failedReducers.add(reduceTask);
		return this;
	}

}
