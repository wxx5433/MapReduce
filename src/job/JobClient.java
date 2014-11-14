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

public class JobClient implements MyTool {
	
	private JobTrackerService jobTrackerService;
	private NameNodeService nameNodeService;
	private JobID jobID;
	private JobConf conf;

	public JobClient() {
		JobTrackerService jobTrackerService = getJobTrackerService();
		// get jobID from jobTracer
		jobID = jobTrackerService.getJobID();
		// get NameNodeService
		NodeID nameNodeID = new NodeID(Configuration.masterIP, Configuration.masterPort);
		this.nameNodeService = Service.getNameNodeService(nameNodeID);
	}

	public JobClient(JobConf configuration) {
		this();
		this.conf = configuration;
	}

	public static RunningJob runJob(JobConf job) {
		JobClient jc = new JobClient(job);
		RunningJob rj = jc.submitJobInternal(job);
		return rj;
	}

	public RunningJob submitJobInternal(JobConf jobConf) {
		jobTrackerService.submitJob(jobConf);
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
	
	@Override
	public void setConf(MyConfiguration conf) {
		// TODO Auto-generated method stub

	}

	@Override
	public MyConfiguration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
