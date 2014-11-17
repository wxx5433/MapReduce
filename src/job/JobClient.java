package job;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import nameNode.NameNodeService;
import node.NodeID;
import jobtracker.JobTrackerService;
import configuration.Configuration;
import configuration.MyConfiguration;
import dfs.Service;
import tool.MyTool;

public class JobClient {
	
	private JobTrackerService jobTrackerService;
	private NameNodeService nameNodeService;
	private JobID jobID;
	private JobConf conf;

	public JobClient() {
		// get JobTrackerService
		jobTrackerService = getJobTrackerService();
		// get NameNodeService
		NodeID nameNodeID = new NodeID(Configuration.masterIP, Configuration.masterPort);
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
		// get jobID from jobTracer
		jobID = jobTrackerService.getJobID();
		// submit the job to jobTracker and get back jobStatus
		JobStatus jobStatus = jobTrackerService.submitJob(jobID, jobConf);
		RunningJob info = new RunningJob(jobStatus);
		return info;
	}

	private JobTrackerService getJobTrackerService() {
		NodeID masterNodeID = new NodeID(Configuration.masterIP, Configuration.masterPort);
		try {
			Registry registry = LocateRegistry.getRegistry(masterNodeID.getIp());
			String name = "rmi://" + masterNodeID.toString() + "/JobTrackerService";
			return (JobTrackerService) registry.lookup(name);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
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
