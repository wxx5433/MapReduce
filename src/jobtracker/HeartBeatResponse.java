package jobtracker;

import java.io.Serializable;
import java.util.ArrayList;

import task.MapTask;
import task.ReduceTask;

public class HeartBeatResponse implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1856392257096807112L;
	private ArrayList<MapTask> newMappers = new ArrayList<MapTask>();
	private ArrayList<ReduceTask> newReducers = new ArrayList<ReduceTask>();

	public void addNewMapper(MapTask mapTask) {
		newMappers.add(mapTask);
	}

	public void addNewReducer(ReduceTask reduceTask) {
		newReducers.add(reduceTask);
	}

	public ArrayList<MapTask> getNewMappers() {
		return newMappers;
	}

	public void setNewMappers(ArrayList<MapTask> newMappers) {
		this.newMappers = newMappers;
	}

	public ArrayList<ReduceTask> getNewReducers() {
		return newReducers;
	}

	public void setNewReducers(ArrayList<ReduceTask> newReducers) {
		this.newReducers = newReducers;
	}

}
