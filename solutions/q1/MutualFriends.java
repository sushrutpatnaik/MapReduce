import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class MutualFriends {

	public static class MutualMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text mapOutputKey = new Text();
		private Text mapOutputValue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			//splits the the input value on basis of tab and removes white spaces and converts into a string
			String []line = value.toString().trim().split("\t");
			//line[0] has the userID and line[1] has the friend list 
			String userId = line[0];
			if(line.length==2) {
				mapOutputValue.set(line[1]);
				//splits the friend list by "," and stores in array friends[]
				String[] friends = line[1].split(",");
				for (String data : friends) {
					int user = Integer.parseInt(userId);
					int friend = Integer.parseInt(data);
					// this condition ensures that the output is always in the form (i,j)
					if(user < friend) 
						mapOutputKey.set(user+","+friend);
					else
						mapOutputKey.set(friend+","+user);
					
					context.write(mapOutputKey, mapOutputValue);
					
				}
			}
		}
	}
	
	public static class MutualReduce extends Reducer<Text, Text, Text, Text>{
		private Text reduceOutputKey = new Text();
		private Text reduceOutputValue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder friendLists = new StringBuilder();
			Text []friendListsArray = new Text[2];
			int i=0;
			Iterator<Text> itr = values.iterator();
			
			while(itr.hasNext()) {
				friendListsArray[i] = new Text();
				friendListsArray[i].set(itr.next());
				i++;
			}
			String user1Friends[] = friendListsArray[0].toString().split(",");
			String user2Friends[] = friendListsArray[1].toString().split(",");
			int count = 0;
			for(int a=0;a<user1Friends.length;a++) {
				for(int b=0;b<user2Friends.length;b++) {
					if(user1Friends[a].equals(user2Friends[b])) {
						friendLists.append(user1Friends[a]+ ',');
						++count;
					}
				}
			}
			if (friendLists.lastIndexOf(",") > -1) {
				friendLists.deleteCharAt(friendLists.lastIndexOf(","));
			}
		//	friendLists = friendLists.substring(0, friendLists.length()-2);
		//	friendLists.insert(0,count + "\t");
			reduceOutputKey = key;
			if(count!=0)
			{	
				reduceOutputValue.set(friendLists.toString());
				context.write(reduceOutputKey,reduceOutputValue);
				
			}
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriends <in> <out>");
			System.exit(2);
		}
		
		conf.set("ARGUMENT",otherArgs[1]);

		// create a job with name "wordcount"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "mutualfriendslist and count");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(MutualMap.class);
		job.setReducerClass(MutualReduce.class);

		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
	
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
