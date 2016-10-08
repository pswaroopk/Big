package problem4;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Collections;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class JobChaining {

	public static class MapA extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] friendList = value.toString().split("\t");
			String person = friendList[0];

			if (friendList.length == 2) {
				List<String> fList = Arrays.asList(friendList[1].split(","));
				for (String str : fList) 
					context.write(new Text(str), new Text(person + "*"));
			}
		}
	}

	public static class MapB extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] userData = value.toString().split(",");
			String userid = userData[0];
			Date bDay = null;
			try {
				bDay = (Date) new SimpleDateFormat("MM/dd/yyyy").parse(userData[9]);
			} catch (ParseException e) {
			    System.out.println("Error parsing date");
			}
			Calendar dob = Calendar.getInstance();
			dob.setTime(bDay);            
			Calendar curDate = Calendar.getInstance();
			
			int ageOfUser =  curDate.get(Calendar.YEAR) - dob.get(Calendar.YEAR); 

			if (userData.length == 10) 
				context.write(new Text(userid), new Text( String.valueOf(ageOfUser) + "B") ); 									// (2,

		}
	}

	public static class ReduceA extends Reducer<Text, Text, Text, Text> {

		HashMap<String, Integer> hMap = new HashMap<>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String id = "";
			int age = 0;

			for (Text val : values) {
				if (val.toString().contains("*")) {
					id = val.toString().substring(0, val.toString().length() - 1);
				} else {
					age = Integer.parseInt(val.toString().substring(0, val.toString().length() - 1));
				}
			}
			if (hMap.containsKey(id)) {
				hMap.put(id, Math.max(hMap.get(id), age));
			} else {
				hMap.put(id, age);
			}

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<String, Integer> entry : hMap.entrySet()) {
				context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
			}
		}
	}

	public static class MapC extends Mapper<LongWritable, Text, Text, Text> { 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] userData = value.toString().split(",");
			if (userData.length == 10) 
				context.write(new Text(userData[0]), new Text(userData[3] + "," + userData[4] + "," + userData[5]));
		}
	}

	public static class MapD extends Mapper<LongWritable, Text, Text, Text> { 
		LinkedList<String> fList = new LinkedList<>();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] userData = value.toString().split("\t");
			fList.add(new String(userData[0] +" "+userData[1]));
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {

			Collections.sort(fList, Collections.reverseOrder());
			for (int i = 0; i <= 10; i++) {
				String temp = fList.get(i);
				String[] result = temp.split(" ");
				context.write(new Text(result[1]), new Text(result[0] + "--"));
			}
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder result = new StringBuilder("");
			String age = "";
			String address = "";
			int count = 0;
			for (Text val : values) {
				if (val.toString().contains("--")) 
					age = val.toString().substring(0, val.toString().length() - 2);
				else 
					address = val.toString();
				count++;
			}
			result.append("," + address + "," + age);
			if (count == 2)
				context.write(new Text(key.toString() + (result.toString())), new Text());
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length < 3) {
			System.err.println("Usage: <friends list> <user info> <out> ");
			System.exit(2);
		}

		// create a job with name

		Job jobA = Job.getInstance(conf, "JobChaining");
		jobA.setJarByClass(JobChaining.class);
		jobA.setMapperClass(MapA.class);
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(jobA, new Path(otherArgs[0]), TextInputFormat.class, MapA.class);
		MultipleInputs.addInputPath(jobA, new Path(otherArgs[1]), TextInputFormat.class, MapB.class);
		jobA.setMapOutputKeyClass(Text.class);
		jobA.setMapOutputValueClass(Text.class);
		jobA.setReducerClass(ReduceA.class);
		jobA.setNumReduceTasks(1);
		String job1OutPath = otherArgs[2] + "/jobA";
		FileOutputFormat.setOutputPath(jobA, new Path(job1OutPath));

		jobA.waitForCompletion(true);

		Job jobB = Job.getInstance(new Configuration(), "JobChaining2");
		jobB.setJarByClass(JobChaining.class);
		jobB.setMapperClass(MapC.class);
		jobB.setOutputKeyClass(Text.class);
		jobB.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(jobB, new Path(otherArgs[1]), TextInputFormat.class, MapC.class);
		MultipleInputs.addInputPath(jobB, new Path(job1OutPath), TextInputFormat.class, MapD.class);
		jobB.setMapOutputKeyClass(Text.class);
		jobB.setMapOutputValueClass(Text.class);
		jobB.setReducerClass(Reduce2.class);
		jobB.setNumReduceTasks(1);
		String job2OutPath = otherArgs[2] + "/jobB";
		FileOutputFormat.setOutputPath(jobB, new Path(job2OutPath));
		jobB.waitForCompletion(true);
	}
}
