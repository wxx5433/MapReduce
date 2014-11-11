package task;

//public class MapTaskThread extends Thread
//{
//	private DataNode dataNode;
//	private MapTask task;
//	private Slave slave;
//	
//	// each of this thread is a mapper task.
//	public MapTaskThread(DataNode dn, MapTask mt, Slave _slave)
//	{
//		dataNode = dn;
//		task = mt;
//		slave = _slave;
//	}
//	
//	public void run()
//	{
//		try
//		{
//			String inputFile = task.getInputFile();
//			String outputFile = task.getOutputFile();
//			MapReduceJob job = task.getJob();
//			FilePartition fp = task.getPartition();
//			
//			slave.addTempFiles(outputFile, job.getJobID());
//			
//			File file = new File(fp.getFileName());
//			if(!file.exists())
//			{
//				System.out.println("file does not exist!");
//				dataNode.readDFS(fp.getFileName());
//			}
//			
//			RecordReader reader = new RecordReader(fp.getFileName(), job.getRecordSize());
//			String[][] inputs = reader.getKeyValuePairs(fp.getPartitionIndex(), fp.getPartitionSize());
//			List<String[]> outputs = new LinkedList<String[]>();
//	        
//	        //perform the mapping
//	        for (int i = 0; i < inputs.length; i++) {
//	                List<String[]> temp = job.map(inputs[i][0], inputs[i][1]);
//	                if(temp != null)
//	                	outputs.addAll(temp);
//	        }
//	        
//	        RecordWriter writer = new RecordWriter(outputFile, job.getRecordSize());
//	        writer.writeRecord(outputs);
//	        
//	        shuffle(outputs, task);
//	        System.out.println("Map Task " + task.getTaskID() + " Done!");
//	        Thread.sleep(100);
//	        
//	        Socket socket = null;
//	        try{
//	        	socket = new Socket(Configuration.masterIP, Configuration.masterPort);
//	        }
//	        catch(Exception e)
//	        {
//	        	System.out.println("creating socket error!");
//	        }
//	        //ObjectInputStream oisMaster = new ObjectInputStream(socket.getInputStream());
//	        ObjectOutputStream oosMaster = new ObjectOutputStream(socket.getOutputStream());
//			
//			Message response = new Message();
//			response.withOp("mapTaskDone")
//			.withTask(task);
//			oosMaster.writeObject(response);
//			oosMaster.flush();
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//	}
//
//	// shuffle the output file to generate input for reducers.
//	public void shuffle(List<String[]> outputs, Task task)
//	{
//		int reduceNum = Configuration.reduceNum;
//		MapReduceJob job = task.getJob();
//		String jobName = job.getJobName();
//		int jobID = job.getJobID();
//	
//		String key;
//		int mapKey;
//		HashMap<Integer, List<String[]>> hashMap = new HashMap<Integer, List<String[]>>(reduceNum);
//	
//		for(int i = 0; i < reduceNum; i++)
//		{
//			List<String[]> list = new LinkedList<String[]>();
//			hashMap.put(i, list);
//		}
//	
//		List<String[]> temp;
//		String[] record;
//		for(int i = 0; i < outputs.size(); i++)
//		{
//			record = outputs.get(i);
//			key = record[0];
//			if(key.length() > 0)
//				mapKey = key.charAt(0) % reduceNum;
//			else
//				mapKey = 0;
//		
//			temp = hashMap.get(mapKey);
//			temp.add(record);
//		}
//	
//		String outputFileName;
//		int mapId = task.getTaskID();
//		for(int i = 0; i < reduceNum; i++)
//		{
//			temp = hashMap.get(i);
//			outputFileName = "Job" + jobID + "_Task" + mapId + "_MapOutput_ForReducer" + i;
//			slave.addTempFiles(outputFileName, jobID);
//			RecordWriter writer = new RecordWriter(outputFileName, job.getRecordSize());
//			writer.writeRecord(temp);
//		}
//	}
// }
