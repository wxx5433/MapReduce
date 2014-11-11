package task;

import java.io.RandomAccessFile;

public class RecordReader {
	private String fileName;
	private int recordLen;

	public RecordReader(String fn, int len) {
		fileName = fn;
		recordLen = len;
	}

	public int getRecordNum() {
		long num = 0;
		try {
			RandomAccessFile raf;
			raf = new RandomAccessFile(fileName, "r");
			while (raf.readLine() != null)
				num++;
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return (int) num;
	}

	public String[][] getKeyValuePairs(int partitionIndex, int partitionSize) {
		RandomAccessFile raf = null;
		String[][] pairs = null;
		try {
			int count = 0;
			raf = new RandomAccessFile(fileName, "r");

			while (count < partitionIndex) {
				count++;
				raf.readLine();
			}

			pairs = new String[partitionSize][2];
			String record;

			for (int i = 0; i < partitionSize; i++) {
				record = raf.readLine();
				if (record == null)
					break;

				pairs[i][0] = ((Integer) (count + i)).toString();
				pairs[i][1] = record;
			}

			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return pairs;
	}

	public boolean nextKeyValue() {
		// TODO Auto-generated method stub
		return false;
	}

	public String getCurrentKey() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getCurrentValue() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean nextKey() {
		// TODO Auto-generated method stub
		return false;
	}

	public Iterable<String> getValues() {
		// TODO Auto-generated method stub
		return null;
	}
}
