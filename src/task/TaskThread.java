package task;

import java.io.IOException;

public class TaskThread extends Thread {
	private Task task;

	// each of this thread is a map/reduce task.
	public TaskThread(Task task) {
		this.task = task;
	}

	public void run() {
		try {
			task.run();
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
