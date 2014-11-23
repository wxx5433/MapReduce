package dataNode;

import java.rmi.RemoteException;

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
	private DataNode dataNode;
	private boolean stop;
	private int interval;

	public HeartBeatThread(DataNode dataNode) {
		this.dataNode = dataNode;
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
			try {
				dataNode.sendHeartbeat();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void terminate() {
		stop = true;
	}

}
