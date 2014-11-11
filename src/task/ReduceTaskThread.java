package task;

// each of this thread is a reduce task
//public class ReduceTaskThread extends Thread
//{
//	private ReduceTask task;
//	private DataNode dataNode;
//	private Slave slave;
//	public ReduceTaskThread(DataNode dn, ReduceTask rt, Slave _slave)
//	{
//		dataNode = dn;
//		task = rt;
//		slave = _slave;
//	}
//	
//	public void run()
//	{
//		try
//		{
//			ArrayList<String> inputFiles = task.getInputFiles();
//			String sortedFileName = sort(inputFiles, task.getReducerID());
//			int jobID = task.getJob().getJobID();
//			
//			for(String fileName : inputFiles)
//			{
//				slave.addTempFiles(fileName, jobID);
//			}
//			slave.addTempFiles(sortedFileName, jobID);
//			
//			MapReduceJob job = task.getJob();
//			String outputFile = task.getOutputFile();
//			
//			FileReader fr = new FileReader(sortedFileName);
//			BufferedReader reader = new BufferedReader(fr);
//			String line;
//			String key = null;
//			ArrayList<String> values = new ArrayList<String>();
//			ArrayList<String> outputs = new ArrayList<String>();
//			while((line = reader.readLine()) != null)
//			{
//				String[] strList = line.split("\t");
//				String curKey = strList[0];
//				String curValue = strList[1];
//				// the key is still continuing
//				if(curKey.equals(key))
//				{
//					values.add(curValue);
//				}
//				else
//				{
//					if(key == null)		// first time
//					{
//						values.add(curValue);
//						key = curKey;
//					}
//					else		// not first time. it means the key changes.
//					{
//						// do the reduce.
//						String output = job.reduce(key, values);
//						outputs.add(output);
//						
//						key = curKey;
//						values.clear();
//						values.add(curValue);
//					}
//				}
//			}
//			reader.close();
//			FileWriter fw = new FileWriter(outputFile);
//			BufferedWriter writer = new BufferedWriter(fw);
//			for(String resultLine : outputs)
//			{
//				writer.write(resultLine);
//				writer.flush();
//			}
//			Thread.sleep(100);
//			writer.close();
//			dataNode.writeDFS(outputFile);
//			System.out.println("Reduce Task " + task.getTaskID() + " Done!");
//			
//			
//			Socket socket = new Socket(Configuration.masterIP, Configuration.masterPort);
//			//ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
//			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//			
//			Message response = new Message();
//			response.withOp("reduceTaskDone")
//			.withTask(task);
//			oos.writeObject(response);
//			oos.flush();
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
//	
//	// before doing the actual reduce, we need to sort the input
//	public String sort(ArrayList<String> fileName, int reducerID) throws Exception
//	{
//		int fileNum = fileName.size();
//		
//		String line;
//		String[] pair = new String[2];
//		HashMap<String, ArrayList<String>> record = new HashMap<String, ArrayList<String>>();
//		ArrayList<String> valueList;
//		for(int i = 0; i < fileNum; i++)
//		{
//			FileReader fr = new FileReader(fileName.get(i));
//			BufferedReader br = new BufferedReader(fr);
//			while((line = br.readLine()) != null)
//			{
//				pair = line.split("\t");
//				if(record.containsKey(pair[0]))
//					valueList = record.get(pair[0]);
//				else
//				{
//					valueList = new ArrayList<String>();
//					record.put(pair[0], valueList);
//				}
//				
//				valueList.add(pair[1]);
//			}
//			br.close();
//		}
//
//		StringBuffer sb = new StringBuffer();
//		Object[] keyArray = record.keySet().toArray();
//		Arrays.sort(keyArray);
//		for(Object key : keyArray)
//		{
//			valueList = record.get(key);
//			for(int i = 0; i < valueList.size(); i++)
//			{
//				line = key + "\t" + valueList.get(i);
//				sb.append(line + "\n");
//			}
//		}
//		
//		String retFileName = "Job" + task.getJob().getJobID() + "_forReducer" + reducerID;
//		FileWriter fw = new FileWriter(retFileName);
//		BufferedWriter bw = new BufferedWriter(fw);
//		bw.write(sb.toString());
//		
//		bw.close();
//		fw.close();
//		
//		return retFileName;
// }
