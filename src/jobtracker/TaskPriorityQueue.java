package jobtracker;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import job.JobID;
import task.MapTask;
import task.ReduceTask;
import task.Task;

public class TaskPriorityQueue extends PriorityQueue<Task> {

	private static final long serialVersionUID = 1230215573089513068L;
	
	private PriorityQueue<Task> taskQueue;
	
	public TaskPriorityQueue(int initialCapacity) {
		taskQueue = new PriorityQueue<Task>(initialCapacity, new TaskComparator());
	}

	private class TaskComparator implements Comparator<Task> {
		@Override
		public int compare(Task o1, Task o2) {
			JobID jobID1 = o1.getJob().getJobID();
			JobID jobID2 = o2.getJob().getJobID();
			int jobIDCompare = jobID1.compareTo(jobID2);
			if (jobIDCompare == 0) {   // same job, map tasks first
				boolean o1Map = o1 instanceof MapTask;
				boolean o2Map = o2 instanceof ReduceTask;
				if (o1Map && !o2Map) {  // o1 Map, o2 Reduce
					return -1;
				} else if (!o1Map && o2Map) {  // o1 Reduce, o2 Map
					return 1;
				} else {  // order does not matter if both are Map/Reduce
					return 0;
				}
			} else {   // FIFO, jobs with smaller ids first
				return jobIDCompare;
			}
		}
	}

	@Override
	public boolean add(Task e) {
		return taskQueue.add(e);
	}

	@Override
	public boolean offer(Task e) {
		return taskQueue.offer(e);
	}

	@Override
	public Task peek() {
		return taskQueue.peek();
	}

	@Override
	public boolean remove(Object o) {
		return taskQueue.remove(o);
	}

	@Override
	public boolean contains(Object o) {
		return taskQueue.contains(o);
	}

	@Override
	public Object[] toArray() {
		return taskQueue.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return taskQueue.toArray(a);
	}

	@Override
	public Iterator<Task> iterator() {
		return taskQueue.iterator();
	}

	@Override
	public int size() {
		return taskQueue.size();
	}

	@Override
	public void clear() {
		taskQueue.clear();
	}

	@Override
	public Task poll() {
		return taskQueue.poll();
	}

	@Override
	public Comparator<? super Task> comparator() {
		return taskQueue.comparator();
	}
	
}
