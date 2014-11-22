package job;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import nameNode.NameNodeService;
import node.NodeID;
import jobtracker.JobTrackerService;
import configuration.Configuration;
import dfs.Service;

public class JobClient {

	private JobTrackerService jobTrackerService;
	private NameNodeService nameNodeService;
	private JobID jobID;
	private JobConf conf;
	private Configuration configuration;

	public JobClient() {
		// get JobTrackerService
		configuration = new Configuration();
		jobTrackerService = getJobTrackerService();
		// get NameNodeService
		NodeID nameNodeID = new NodeID(configuration.nameNodeIP, 
				configuration.nameNodePort);
		this.nameNodeService = Service.getNameNodeService(nameNodeID);
	}

	public JobClient(JobConf configuration) {
		this();
		this.conf = configuration;
	}

	//	public static RunningJob runJob(JobConf job) {
	//		JobClient jc = new JobClient(job);
	//		RunningJob rj = jc.submitJobInternal(job);
	//		return rj;
	//	}

	public RunningJob submitJobInternal(JobConf jobConf) {
		System.out.println("Mapper class: " +  jobConf.getMapperClass());
		// get jobID from jobTracer
		try {
			jobID = jobTrackerService.getJobID();
			jobConf.setJobId(jobID);
		} catch (RemoteException e1) {
			System.out.println("Fail to get job id from job tracker");
			e1.printStackTrace();
			return null;
		}
		if (jobID == null) {
			System.out.println("Launch job failed: cannot get new jobID");
			return null;
		}

		CheckMessage checkMessage = checkJobConf(jobConf);
		if (!checkMessage.isValid()) {
			System.out.println("Invalid job configuration:" + checkMessage.getMessage());
			return null;
		}

		// submit the job to jobTracker and get back jobStatus
		JobStatus jobStatus = null;
		try {
			jobStatus = jobTrackerService.submitJob(jobID, jobConf);
		} catch (RemoteException e) {
			System.out.println("Fail to submit job to jobTracker");
			e.printStackTrace();
			return null;
		}
		RunningJob info = new RunningJob(this, jobStatus);
		return info;
	}

	public JobTrackerService getJobTrackerService() {
		NodeID jobTrackerNodeID = new NodeID(configuration.jobTrackerIP,
				configuration.jobTrackerPort);
		try {
			Registry registry = LocateRegistry.getRegistry(jobTrackerNodeID.getIp());
			String name = "rmi://" + jobTrackerNodeID.toString() + "/JobTrackerService";
			return (JobTrackerService) registry.lookup(name);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private CheckMessage checkJobConf(JobConf jobConf) {
		if (jobConf.getInputPath() == null) {
			return new CheckMessage(false, "No input path");
		}

		try {
			if (!nameNodeService.containsFile(jobConf.getInputPath())) {
				return new CheckMessage(false, "Input files do not exist in DFS");
			}
		} catch (RemoteException e) {
			System.out.println("Failt to contact nameNode service when checking input path");
			e.printStackTrace();
		}

		if (jobConf.getOutputPath() == null) {
			return new CheckMessage(false, "No output path");
		}

		// input format

		// output format

		return new CheckMessage(true);
	}

	private class CheckMessage {
		private boolean valid;
		private String message;

		public CheckMessage(boolean valid) {
			setValid(valid);
		}

		public CheckMessage(boolean valid, String message) {
			this(valid);
			setMessage(message);
		}

		public boolean isValid() {
			return valid;
		}

		public void setValid(boolean valid) {
			this.valid = valid;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}
	}

}
