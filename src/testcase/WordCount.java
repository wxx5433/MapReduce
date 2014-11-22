package testcase;

import inputformat.LineInputFormat;

import java.io.IOException;

import job.Job;
import outputformat.LineOutputFormat;
import partitioner.HashPartitioner;
import tool.Mapper;
import tool.MyTool;
import tool.MyToolRunner;
import tool.Reducer;

public class WordCount implements MyTool {

	/**
	 * Maps each line and output the filter result with data format needed
	 */
	public static class PageRankMapper extends
			Mapper<Long, String, String, String> {

		public void map(Long key, String value, Context context)
				throws IOException, InterruptedException {
			String data = value.toString();
			String[] dataArray = data.split("\t");
			for (String dataValue : dataArray) {
				context.write(dataValue, "1");
			}
		}
	}

	/**
	 * Performs PageRank collection of all the values for each key.
	 */
	public static class PageRankReducer extends Reducer {

		/**
		 * Collect all the URLs within and writes them to the same key.
		 * 
		 */
		public void reduce(String key, Iterable<String> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (String value : values) {
				++sum;
			}
			context.write(key, Integer.toString(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		MyToolRunner.run(new WordCount(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			return 0;
		}
		String input = args[0];
		String output = args[1];
		System.out.println("input:" + input);
		System.out.println("output:" + output);
		Job job = new Job("WordCount");
		job.setMapperClass(PageRankMapper.class);
		job.setMapOutputKeyClass(String.class);
		job.setMapOutputValueClass(String.class);
		job.setReducerClass(PageRankReducer.class);
		job.setNumReduceTasks(2);
		job.setInputFormatClass(LineInputFormat.class);
		job.setOutputFormatClass(LineOutputFormat.class);
		job.setInputPath(input);
		job.setOutputPath(output);
		job.setPartitonerClass(HashPartitioner.class);
		boolean result = job.waitForCompletion(true);
		return (result ? 0 : 1);
	}
}