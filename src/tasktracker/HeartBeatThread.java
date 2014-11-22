package tasktracker;

import configuration.Configuration;

/**
 * This class is used to initialize a socket thread for slave nodes to have a
 * socked connection with master node.
 * 
 * @author Xiaoxiang Wu(xiaoxiaw)
 * @author Ye Zhou(yezhou)
 *
 */
public class HeartBeatThread implements Runnable {
	private TaskTracker taskTracker;
	private boolean stop;
	private int interval;

	public HeartBeatThread(TaskTracker taskTracker) {
		this.taskTracker = taskTracker;
		stop = false;
		interval = new Configuration().heartBeatInterval;
	}

	@Override
	public void run() {
		while (!stop) {
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			taskTracker.sendHeartbeat();
		}
	}

	public void terminate() {
		stop = true;
	}

}
