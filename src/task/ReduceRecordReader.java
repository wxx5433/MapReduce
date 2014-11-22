package task;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class ReduceRecordReader<KEYIN, VALUEIN> {

	private BufferedReader br;
	private String key;
	private String value;
	private ValueIterable valueIterable;

	public ReduceRecordReader() {
		key = null;
		value = null;
	}

	public void initialize(String reduceInputPath) throws FileNotFoundException {
		// fileName here is the split fileName, not the original fileName
		FileReader fr = null;
		System.out.println("reduce input file path: " + reduceInputPath);
		fr = new FileReader(reduceInputPath);
		br = new BufferedReader(fr);
	}

	public boolean nextKey() throws IOException {
		String line;
		line = br.readLine();
		System.out.println("reducer record reader read one line: " + line);
		if (line == null) {
			return false;
		}
		String[] data = line.split("\t");
		key = data[0];
		value = line.substring(key.length() + 1, line.length());
		valueIterable = new ValueIterable(value.split("\t"));
		return true;
	}

	public String getCurrentKey() {
		return key;
	}

	public String getCurrentValue() {
		return value;
	}

	public Iterable<String> getCurrentValues() {
		return valueIterable;
	}

	public void close() throws IOException {
		br.close();
	}

	class ValueIterable implements Iterable<String> {
		public String[] valueData = null;

		public ValueIterable(String[] arr) {
			valueData = arr;
		}

		public ValueIterator iterator() {
			return new ValueIterator(this);
		}

		public class ValueIterator implements Iterator<String> {
			private ValueIterable valueIterable = null;
			private int count = 0;

			public ValueIterator(ValueIterable m) {
				valueIterable = m;
			}

			public boolean hasNext() {
				if (count < valueIterable.valueData.length) {
					return true;
				} else {
					return false;

				}
			}

			public String next() {
				int t = count;
				count++;
				return valueIterable.valueData[t];
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		}
	}

}
