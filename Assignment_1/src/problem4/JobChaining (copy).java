package problem3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
//import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InMemoryJoin {
//	public static String userA = "";
//	public static String userB = "";
	

	public static class MapA extends
			Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {

		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); // type of output key
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] friendList = value.toString().split("\t");
			long uID = Long.parseLong(friendList[0]);
			if (friendList.length > 1) {
				String[] userFriends = friendList[1].split(",");
				//word.set(friendList[1]);
				for (String data : userFriends) {
					// word.set(data); // set word as each input keyword
					context.write(new LongWritable(Long.parseLong(data)),new LongWritable(uID)); // create a pair <keyword, 1>
				}			
			}
		}	
	}
	
	public static class MapB extends
	Mapper<LongWritable, Text, LongWritable, Text> {
	private Text word = new Text(); // type of output key
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] userData = value.toString().split(",");
		StringBuilder sb = new StringBuilder();
		long uID = Long.parseLong(userData[0]);
		Date DOB = null;
        try {
            DOB = (Date) new SimpleDateFormat("MM/dd/yyyy").parse(userData[9]);
        } catch (ParseException e) {
            System.out.println("Error parsing date");
        }
        Calendar dob = Calendar.getInstance();
        dob.setTime(DOB);            
        Calendar curDate = Calendar.getInstance();

        int ageOfUser =  curDate.get(Calendar.YEAR) - dob.get(Calendar.YEAR); 
		sb.append(userData[3] + "," + ageOfUser);
		
		word.set(sb.toString());
		context.write(new LongWritable(Long.parseLong(userData[0])), word); // create a pair <keyword, 1>
		
		
	}
	
	


	public static class Reduce extends Reducer<PairWritable, Text, PairWritable, Text> {
		private Text result = new Text(); // type of output key
		protected HashMap<String, String>hMap = new HashMap<String, String>();
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("userdataFilepath");
			Path pt = new Path("hdfs://" + filePath);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String[] columns = br.readLine().split(",");
			while (columns != null & columns.length > 0 ) {
				hMap.put(columns[0], columns[1] + " " + ":" + columns[9]);
				if(br.readLine() != null)
					columns = br.readLine().split(",");
				else 	
					break;
			}		
			
		}
		public void reduce(PairWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			List<List<String>> fLists = new ArrayList<>();

			for (Text friends : values) 
				fLists.add(new LinkedList<String>(Arrays.asList(friends.toString().split(","))));
				
			if (fLists.size() > 2) {
				sb.append("Ekkado dobbindi");
			} else {				
				fLists.get(0).retainAll(fLists.get(1));
				if(fLists.get(0).size() > 0){
					for (String str : fLists.get(0)) {
						if(hMap.containsKey(str))
							sb.append(hMap.get(str) + ", ");
					}
					sb.setLength(sb.length() - 2);					
				}
				else
					sb.append("No mutual Friends");
			}
			result.set(sb.toString());
			context.write(key, result); // create a pair <pair, list of friends>
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get all args
		if (otherArgs.length != 5) {
			System.err.println("Usage: InMemoryJoin <in1> <in2> <out> <userA> <userB>");
			System.exit(2);
		}
		conf.set("userdataFilepath", otherArgs[2].toString());
		conf.set("userA", otherArgs[3].toString());
		conf.set("userB", otherArgs[4].toString());
		
		// create a job with name "mutualFriends"
		Job job = new Job(conf, "InMemoryJoin");
		job.setJarByClass(InMemoryJoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);
		// set output key type
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(PairWritable.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
