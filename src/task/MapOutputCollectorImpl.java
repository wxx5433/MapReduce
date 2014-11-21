package task;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import configuration.ConfigurationStrings;

public class MapOutputCollectorImpl<K, V> implements MapOutputCollector<K, V> {

	@SuppressWarnings("rawtypes")
	HashMap<Integer, RecordWriter> mapOutputMap = new HashMap<Integer, RecordWriter>();
	ArrayList<String> outputPaths = new ArrayList<String>();

	public void init(CollectorContext context) throws FileNotFoundException {
		int reduceNum = context.getJobConf().getNumReduceTasks();
		String mapOutputPath = ConfigurationStrings.MAP_OUTPUT_PATH;
		String outputFilePath = mapOutputPath
				+ context.getMapTask().getTaskID().toString();
		initHashMap(reduceNum, outputFilePath);
	}

	private void initHashMap(int reduceNum, String outputFileDirPath)
			throws FileNotFoundException {
		for (int i = 0; i < reduceNum; i++) {
			String outputPath = outputFileDirPath + "_" + reduceNum;
			outputPaths.add(outputPath);
			DataOutputStream out = new DataOutputStream(new FileOutputStream(
					outputPath));
			@SuppressWarnings("rawtypes")
			RecordWriter recordWriter = new LineRecordWriter(out);
			mapOutputMap.put(reduceNum, recordWriter);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void collect(K key, V value, int partition) throws IOException {
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
