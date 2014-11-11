package jobtracker;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class JobTracker extends UnicastRemoteObject implements
		JobSubmissionInterface {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1872391379768700832L;

	protected JobTracker() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}

}
