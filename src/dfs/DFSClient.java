package dfs;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nameNode.NameNodeService;
import node.NodeID;
import configuration.Configuration;
import dataNode.DataNodeService;
import fileSplit.FileSplit;
import fileSplit.RemoteSplitOperator;

public class DFSClient {

	private NameNodeService nameNodeService = null;
	private Configuration configuration;
	private NodeID nameNodeID;

	public DFSClient() {
		configuration = new Configuration();
		nameNodeID = new NodeID(configuration.nameNodeIP, configuration.nameNodePort);
		nameNodeService = Service.getNameNodeService(nameNodeID);
	}

	/**
	 * Upload file to DFS. The client will split the file into chunks by lines. 
	 * The users specified in the configuration file that how many lines they want in a chunk.
	 * We do not use file size to split file, since it may split one line into two different
	 * chunks, which makes it complicated to deal with. 
	 * @param filePath local path of the upload file
	 * @param fileName the file to upload
	 */
	public void uploadFile(String filePath, String fileName) {
		FileReader fr  = null;
		BufferedReader br = null;
		int splitSize = configuration.splitSize;
		try {
			// read the file from client machine
			fr = new FileReader(filePath + File.separator + fileName);
			br = new BufferedReader(fr);
			// store one chunk's content in the contents list
			//!!!!! there may be some problem if the file is really big. 
			List<String> contents = new ArrayList<String>();
			String line;
			int lineCount = 0;
			while ((line = br.readLine()) != null) {
				++lineCount;
				contents.add(line);
				if (lineCount % splitSize == 0) {
					int blockIndex = lineCount / splitSize;
					// ask the nameNode where to upload the chunks
					Iterable<NodeID> dataNodes = nameNodeService.getDataNodesToUpload(fileName, blockIndex);
					RemoteSplitOperator rso = new RemoteSplitOperator();
					try {
						rso.writeSplit(dataNodes, fileName, blockIndex, contents);
					} catch (Exception e) {
						e.printStackTrace();
					}
					contents.clear();
				}
				// should send response to NameNode here to confirm data transfer success. 
			}
			if (lineCount % splitSize != 0) {
				int blockIndex = lineCount / splitSize + 1;
				// ask the nameNode where to upload the chunks
				Iterable<NodeID> dataNodes = nameNodeService.getDataNodesToUpload(fileName, blockIndex);
				RemoteSplitOperator rso = new RemoteSplitOperator();
				try {
					rso.writeSplit(dataNodes, fileName, blockIndex, contents);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Download file from DFS to client
	 * @param fileName The name of the file to download
	 * @param localPath Local path where the download file should be put.
	 * @param targetName The name to save the file as. 
	 * @throws IOException 
	 */
	public void getFile(String fileName, String localPath, String targetName) {
		NameNodeService nameNodeService = Service.getNameNodeService(nameNodeID);
		// ask the NameNode where all the chunks of this file are located
		try {
			Iterable<FileSplit>	splits = nameNodeService.getDataNodesToDownload(fileName);
			FileWriter fw = new FileWriter(localPath + File.separator + targetName);
			BufferedWriter bw = new BufferedWriter(fw);
			boolean firstLine = true;
			for (FileSplit split: splits) {
				RemoteSplitOperator rso = new RemoteSplitOperator();
				List<String> lines = rso.readSplit(split, localPath, targetName);
				for (String line: lines) {
					if (firstLine) {
						bw.write(line);
						firstLine = false;
					} else {
						bw.write("\n" + line);
					}
				}
			}
			bw.close();
			fw.close();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * List all files on DFS 
	 */
	public void listAllFiles() {
		Map<String, Set<FileSplit>> filesCopy = null;
		try {
			filesCopy = nameNodeService.listAllFiles();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		System.out.println("List all files on the DFS:");
		for (Map.Entry<String, Set<FileSplit>> file: filesCopy.entrySet()) {
			String fileName = file.getKey();
			System.out.println("filename: " + fileName);
			for (FileSplit split: file.getValue()) {
				System.out.println("\tSplit " + split.getBlockIndex() + ":");
				for (String host: split.getHosts()) {
					System.out.println("\t\t" + host + "\t" + split.getPath(host));
				}
			}
		}
	}

	public static void main(String[] args) {
		DFSClient client = new DFSClient();
//		client.uploadFile(".", "test");
		client.listAllFiles();
//		client.getFile("test", ".", "result");
	}
}
