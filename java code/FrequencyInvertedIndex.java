package question_d;

import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;

import java.util.HashSet;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FrequencyInvertedIndex extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new FrequencyInvertedIndex(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "FrequencyInvertedIndex");

		job.setJarByClass(FrequencyInvertedIndex.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text filename = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// to create a HashSet Class of the words we want to block in the map function (the stop words)
			HashSet<String> blocked = new HashSet<String>();
			// use BufferedReader to read every line of the stop words file created with question a
			BufferedReader BR = new BufferedReader(new FileReader(
					new File("/home/cloudera/workspace/InvertedIndex/StopWords/StopWords.txt")));
			// add every stop word to the HashSet Class without the empty one
			String motLu;
			while((motLu = BR.readLine()) != null){
				blocked.add(motLu);
			}
			
			// to identify the file from which the word comes
			String location = ((FileSplit) context.getInputSplit()).getPath().getName();
			filename = new Text(location);

			// in word.set add toLowerCase to put everything in lower case before counting 
			// and remove all non-letter characters
			for (String token : value.toString().split("\\s+")) {
				if (!blocked.contains(token.toLowerCase().replaceAll("[^a-zA-Z ]", ""))) {
					word.set(token.toLowerCase().replaceAll("[^a-zA-Z ]", ""));
				}
			}
			context.write(word, filename);
		}
	}

	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			
		    HashMap<String, Integer> hs_FileName = new HashMap<String, Integer>();
		    for (Text val : values) {
		    	String[] splitted = val.toString().split(" ");
		    	for (int i = 0; i < splitted.length; i++) {
		    		if (!hs_FileName.containsKey(splitted[i])) {
		    			hs_FileName.put(splitted[i], 1);
		    		} 
		    		else {
		    			hs_FileName.put(splitted[i], (Integer) hs_FileName.get(splitted[i]) + 1);
		    		}
	            }
	        }


			// to create the new output
			StringBuilder SB = new StringBuilder();
			String separator = " ";
			for (Object val : hs_FileName.keySet()) {
				SB.append(separator);
				SB.append(val + "#" + (Integer) hs_FileName.get(val));
			}
			
			context.write(key, new Text(SB.toString()));
		}
	}
}

