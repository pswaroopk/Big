package problem3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
//import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Arrays;
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
	

	public static class Map extends
			Mapper<LongWritable, Text, PairWritable, Text> {

		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); // type of output key

		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String userA = context.getConfiguration().get("userA");
			String userB = context.getConfiguration().get("userB");

			String[] friendList = value.toString().split("\t");
			String uID = friendList[0];
			if (friendList.length > 1) {				
				if (uID.compareTo(userA) == 0 && Arrays.asList(friendList[1].split(",")).contains(userB) || 
				    uID.compareTo(userB) == 0 && Arrays.asList(friendList[1].split(",")).contains(userA) ){					
					word.set(friendList[1]);						
					context.write(sortPair(Long.parseLong(userA), Long.parseLong(userB)), word);
				}
			}
		}	
	}

	public static PairWritable sortPair(Long p1, Long p2) {
		return p1 > p2 ? (new PairWritable(p2, p1)): (new PairWritable(p1, p2));}

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
			String line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				hMap.put(arr[0], arr[1] + " " + arr[2] + ":" + arr[9]);
				line = br.readLine();
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