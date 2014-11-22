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

	TaskTracker taskTracker;
	volatile boolean isStop;
	private ExecutorService pool;

	public MapperExecutionExecutor(TaskTracker taskTracker) {
		this.taskTracker = taskTracker;
		this.isStop = false;
		System.out.println(taskTracker.getMapperSlotNumber());
		pool = Executors.newFixedThreadPool(taskTracker.getMapperSlotNumber());
	}

	@Override
	public void run() {
		System.out.println("Mapper execution starts!");
		while (!isStop) {
			try {
				MapTask mapTask = taskTracker.mapTaskQueue.take();
				execute(mapTask, taskTracker);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (isStop)
				break;
		}
	}

	public void execute(MapTask mapTask, TaskTracker taskTracker) {
		TaskThread mapTaskThread = new TaskThread(mapTask, taskTracker);
		Thread thread = new Thread(mapTaskThread);
		System.out.println("Run new mapTasks!");
		pool.execute(thread);
	}

	public void terminate() {
		pool.shutdown();
		isStop = true;
	}

}
