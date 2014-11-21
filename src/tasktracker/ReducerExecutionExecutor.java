package tasktracker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import task.ReduceTask;
import task.TaskThread;

/**
 * {@link ReducerExecutionExecutor} is thread pool management thread to control
 * how many {@link ExecutionThread} threads can run at the same time. Exeuctor
 * will try to get {@link SplitTask} from {@link SplitTasksMangement}, if there
 * is split task, it will use thread pool to lauch a thread.
 * 
 * 
 */

class ReducerExecutionExecutor implements Runnable {

	TaskTracker TaskTracker;
	volatile boolean isStop;
	private ExecutorService pool;

	public ReducerExecutionExecutor(TaskTracker taskTracker) {
		this.TaskTracker = taskTracker;
		this.isStop = false;
		pool = Executors.newFixedThreadPool(taskTracker.getReducerSlotNumber());
	}

	@Override
	public void run() {
		while (!isStop) {
			try {
				ReduceTask reduceTask = TaskTracker.reduceTaskQueue.take();
				execute(reduceTask);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (isStop)
				break;
		}
	}

	public void execute(ReduceTask reduceTask) {
		TaskThread reduceTaskThread = new TaskThread(reduceTask);
		Thread thread = new Thread(reduceTaskThread);
		pool.execute(thread);
	}

	public void terminate() {
		pool.shutdown();
		isStop = true;
	}

}
