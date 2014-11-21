package tasktracker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import task.MapTask;
import task.TaskThread;

/**
 * {@link MapperExecutionExecutor} is thread pool management thread to control
 * how many {@link ExecutionThread} threads can run at the same time. Exeuctor
 * will try to get {@link SplitTask} from {@link SplitTasksMangement}, if there
 * is split task, it will use thread pool to lauch a thread.
 * 
 * 
 */

class MapperExecutionExecutor implements Runnable {

	TaskTracker TaskTracker;
	volatile boolean isStop;
	private ExecutorService pool;

	public MapperExecutionExecutor(TaskTracker taskTracker) {
		this.TaskTracker = taskTracker;
		this.isStop = false;
		pool = Executors.newFixedThreadPool(taskTracker.getMapperSlotNumber());
	}

	@Override
	public void run() {
		while (!isStop) {
			try {
				MapTask mapTask = TaskTracker.mapTaskQueue.take();
				execute(mapTask);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (isStop)
				break;
		}
	}

	public void execute(MapTask mapTask) {
		TaskThread mapTaskThread = new TaskThread(mapTask);
		Thread thread = new Thread(mapTaskThread);
		pool.execute(thread);
	}

	public void terminate() {
		pool.shutdown();
		isStop = true;
	}

}
