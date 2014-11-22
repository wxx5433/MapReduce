package task;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.management.RuntimeErrorException;

public class MapOutputCollectorImpl<K, V> implements MapOutputCollector<K, V> {

	@SuppressWarnings("rawtypes")
	public HashMap<Integer, RecordWriter> mapOutputMap = new HashMap<Integer, RecordWriter>();
	public ArrayList<String> outputPaths = new ArrayList<String>();
	private String outputPath;

	public MapOutputCollectorImpl(String outputPath) {
		this.outputPath = outputPath;
	}

	public void init(CollectorContext context) throws FileNotFoundException {
		int reduceNum = context.getJobConf().getNumReduceTasks();
		String mapOutputPath = outputPath;
		String outputFilePath = mapOutputPath + "/"
				+ context.getMapTask().getTaskID().toString();
		File dir = new File(outputFilePath);
		if (!dir.exists()) {
			if (!dir.mkdirs()) {
				throw new RuntimeErrorException(null, "make dir error!");
			}
		}
		initHashMap(reduceNum, outputFilePath);
	}

	private void initHashMap(int reduceNum, String outputFileDirPath)
			throws FileNotFoundException {
		for (int i = 0; i < reduceNum; i++) {
			String outputPath = outputFileDirPath + "/" + i;
			System.out.println(outputPath);
			outputPaths.add(outputPath);
			DataOutputStream out = new DataOutputStream(new FileOutputStream(
					outputPath));
			@SuppressWarnings("rawtypes")
			RecordWriter recordWriter = new LineRecordWriter(out);
			mapOutputMap.put(i, recordWriter);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void collect(K key, V value, int partition) throws IOException {
		System.out.println("partition: " + partition);
		RecordWriter recordWriter = mapOutputMap.get(partition);
		recordWriter.write(key, value);
	}

	@SuppressWarnings("rawtypes")
	public void close() throws IOException {
		for (Entry<Integer, RecordWriter> m : mapOutputMap.entrySet()) {
			m.getValue().close();
		}
	}

	@Override
	public ArrayList<String> getOutputPaths() {
		return outputPaths;
	}
}
