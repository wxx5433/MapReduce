package task;

import java.io.IOException;

import tasktracker.TaskTracker;

public class TaskThread extends Thread {
	private Task task;
	private TaskTracker taskTracker;

	// each of this thread is a map/reduce task.
	public TaskThread(Task task, TaskTracker taskTracker) {
		this.task = task;
		this.taskTracker = taskTracker;
	}

	public void run() {
		System.out.println("New Task starts!-----"
				+ task.getTaskAttemptID().toString());
		try {
			task.run();
			taskTracker.updateCompletedTask(task);
		} catch (ClassNotFoundException e) {
			taskTracker.updateFailedTaskStatus(task);
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			taskTracker.updateFailedTaskStatus(task);
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			taskTracker.updateFailedTaskStatus(task);
			throw new RuntimeException(e);
		} catch (IOException e) {
			taskTracker.updateFailedTaskStatus(task);
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			taskTracker.updateFailedTaskStatus(task);
			throw new RuntimeException(e);
		}
	}
}
