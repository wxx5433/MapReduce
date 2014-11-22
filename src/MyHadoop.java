import java.io.File;
import java.rmi.RemoteException;

import job.JobClient;
import job.JobID;
import job.JobInfo;
import jobtracker.JobTrackerService;
import nameNode.NameNodeService;
import node.NodeID;
import configuration.Configuration;
import dfs.DFSClient;
import dfs.Service;


public abstract class MyHadoop {

	private static void printHelpInfo() {
		System.out.println("MyHadoop Usage: ");
		System.out.println("\t dfs -- operating distributed file system");
		System.out.println("\t\t get [fileName] -- download file from hdfs to local disk");
		System.out.println("\t\t put [file] -- upload local file to dfs");
		System.out.println("\t\t ls -- list all files on dfs");
		System.out.println("\t job  -- job operations on MyHadoop");
		System.out.println("\t\t list -- list all jobs info on MyHadoop");
		System.out.println("\t\t kill [jobId] -- kill a job on MyHadoop");
	}

	private static void parseCommand(String[] args) {
		// check parameter number
		if (args.length < 2 && args.length > 3) {
			printHelpInfo();
			return;
		}

		String serviceType = args[0];
		Configuration configuration = new Configuration();
		if (serviceType.equals("dfs")) {
			dfs(configuration, args);
		} else if (serviceType.equals("job")) {
			job(configuration, args);
		} else {
			printHelpInfo();
		}
	}

	private static void dfs(Configuration configuration, String[] args) {
		NodeID nameNodeID = new NodeID(configuration.nameNodeIP, 
				configuration.nameNodePort);
		NameNodeService nameNodeService = Service.getNameNodeService(nameNodeID);
		String command = args[1];
		DFSClient dfsClient = new DFSClient();
		if (command.equals("get")) {
			if (args.length < 3) {
				printHelpInfo();
				return;
			}
			String fileName = args[2];
			try {
				if (!nameNodeService.containsFile(fileName)) {
					System.out.println("The file does not exist!");
					return;
				}
				dfsClient.getFile(fileName, System.getProperty("user.dir"), fileName);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		} else if (command.equals("put")) {
			if (args.length < 3) {
				printHelpInfo();
				return;
			}
			String filePath = args[2];
			String fileName = getFileName(filePath);
			if (fileName == null) {
				System.out.println("Invalid fileName");
				return;
			}
			dfsClient.uploadFile(getPath(filePath), fileName, fileName);
		} else if (command.equals("ls")) {
			dfsClient.listAllFiles();
		} else {
			printHelpInfo();
		}
	}
	
	private static String getFileName(String filePath) {
		String[] splits = filePath.split(File.separator);
		if (splits.length == 0) {
			return null;
		}
		return splits[splits.length - 1];
	}
	
	// no fileName append
	private static String getPath(String filePath) {
		String[] splits = filePath.split(File.separator);
		if (splits.length == 0) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		sb.append(splits[0]);
		for (int i = 1; i < splits.length - 1; ++i) {
			sb.append(File.separator);
			sb.append(splits[i]);
		}
		return new String(sb);
	}
	
	private static void job(Configuration configuration, String[] args) {
		JobClient jobClient = new JobClient();
		JobTrackerService jobTrackerService = jobClient.getJobTrackerService();
		String command = args[1];
		if (command.equals("list")) {
			try {
				JobInfo[] jobInfos = jobTrackerService.listAllJobs();
				for (JobInfo jobInfo: jobInfos) {
					System.out.println(jobInfo);
				}
			} catch (RemoteException e) {
				System.out.println("Fail to connect job tracker");
				e.printStackTrace();
			}
		} else if (command.equals("kill")) {
			if (args.length != 3) {
				printHelpInfo();
				return;
			}
			JobID jobId = new JobID(Integer.parseInt(args[2]));
			try {
				jobTrackerService.killJob(jobId);
			} catch (RemoteException e) {
				System.out.println("Fail to connect job tracker");
				e.printStackTrace();
			}
		} else {
			printHelpInfo();
		}
	}

	public static void main(String[] args) {
		parseCommand(args);
	}
}
