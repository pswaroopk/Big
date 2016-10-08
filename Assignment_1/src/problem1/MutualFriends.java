package problem1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

//import com.sun.xml.internal.bind.v2.schemagen.xmlschema.List;


public class MutualFriends {
	
	public static class Map extends Mapper<LongWritable, Text, PairWritable, Text> {
		
		
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); // type of output key
		
		
		//for every friend in friend list 1. create a pair with user and use this as a key for context
		//the value will be the list of friends minus the friend in the pair 'or' we can send the entire 
		//userFriends (list long) as value
		//We need to send multiple <k, v> pairs
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] friendList = value.toString().split("\t");
			long uID = Long.parseLong(friendList[0]);
			String[] userFriends = friendList[1].split(",");			
			word.set(friendList[1]);			
			for (String data : userFriends) {
				//word.set(data); // set word as each input keyword
				context.write(sortPair(uID, Long.parseLong(data)), word); // create a pair <keyword, 1>
			}
		}
	}
	
	public static PairWritable sortPair(Long p1, Long p2) {
		// LongWritable p1 =new LongWritable(f1);
		// LongWritable p2 =new LongWritable(f2);
		if (p1 > p2) {
			return (new PairWritable(p2, p1));

		} else
			return (new PairWritable(p1, p2));

	}

	public static class Reduce extends Reducer<PairWritable, Text, PairWritable, Text> {
		//private PairWritable keyPair = new PairWritable();
		private Text result = new Text(); // type of output key

		//public void reduce(Text key, Iterable<PairWritable> values, Context context)
		public void reduce(PairWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			List<List<String>>fLists = new ArrayList<>();
			//List<String> bList = new ArrayList<>();		
			
			for(Text friends: values){	
				fLists.add(Arrays.asList(friends.toString().split(",")));				
			}
			if(fLists.size() > 2){
				sb.append("Ekkado dobbindi");
			}
			else{
				List<String>aList = fLists.get(0);
				List<String>bList = fLists.get(1);
				
				for(String str: bList){
					if(aList.contains(str))
						sb.append(str + ",");					
				}
					
				if (sb.length() > 0){
					sb.setLength(sb.length() - 1);
				}
				else
					sb.append(" ");		
			}	
			result.set(sb.toString());
			context.write(key, result); // create a pair <pair, list of friends>
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriends <in> <out>");
			System.exit(2);
		}
		// create a job with name "mutualFriends"
		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriends.class);
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
