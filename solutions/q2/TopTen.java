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
public class TopTen {

	public static class TopMap1 extends Mapper<LongWritable, Text, Text, Text>{
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
	
	public static class TopReduce1 extends Reducer<Text, Text, Text, Text>{
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
			friendLists.insert(0,count + "\t");
			reduceOutputKey = key;
			if(count!=0)
			{	
				reduceOutputValue.set(friendLists.toString());
				context.write(reduceOutputKey,reduceOutputValue);
				
			}
		}
	}
	
	public static class TopMap2 extends Mapper<Object, Text, LongWritable, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String [] line = value.toString().split("\\s+");
			context.write(new LongWritable(Integer.parseInt(line[1])), new Text(line[0]+"	"+line[2]));
						
		}
	}
	public static class TopReduce2 extends Reducer<LongWritable, Text, Text, Text>{
		int count;
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text val:values) {
				if(count==10) {
					break;
				}
				String [] valueSplit = val.toString().split("\\s+");
				context.write(new Text(valueSplit[0]), new Text(key.toString()+"	"+valueSplit[1]));
				count++;
			}
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopTen <in> <out>");
			System.exit(2);
		}
		
		conf.set("ARGUMENT",otherArgs[1]);

		// create a job with name "wordcount"
		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "TopTen");
		job1.setJarByClass(TopTen.class);
		job1.setMapperClass(TopMap1.class);
		job1.setReducerClass(TopReduce1.class);

		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
	
		job1.setOutputKeyClass(Text.class);
		// set output value type
		job1.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"_temp"));
		//Wait till job completion
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf, "TopTenMutualFriends");
		job2.setJarByClass(TopTen.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapperClass(TopMap2.class);
		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job2.setNumReduceTasks(1);
		job2.setReducerClass(TopReduce2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"_temp"));
		FileOutputFormat.setOutputPath(job2,  new Path(otherArgs[1]));
		System.exit(job2.waitForCompletion(true)?0:1);
	}

}
