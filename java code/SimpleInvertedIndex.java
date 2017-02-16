package question_b;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
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

public class SimpleInvertedIndex extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new SimpleInvertedIndex(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "SimpleInvertedIndex");

		job.setJarByClass(SimpleInvertedIndex.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");

		job.getConfiguration().setBoolean("mapred.compress.map.output", true);
	    job.getConfiguration().setClass("mapred.map.output.compression.codec",BZip2Codec.class, CompressionCodec.class);
		
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

			// to create a HashSet Class with value the file names
			HashSet<String> hs_FileName = new HashSet<String>();
			for (Text val : values) {
				hs_FileName.add(val.toString());
			}

			// to create the new output
			StringBuilder SB = new StringBuilder();
			String separator = " ";
			for (String val : hs_FileName) {
				SB.append(separator);
				SB.append(val);
			}

			context.write(key, new Text(SB.toString()));

		}
	}
}
