import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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

public class MutualFriendsCity {
	public static class Map extends Mapper<Object, Text, Text, Text>{
		private HashMap<String, String> map = new HashMap<>();
		private Text mapOutputKey = new Text();
		private Text mapOutputValue = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			File file = new File("/home/sushrut/Assignment1/userdata.txt");
			BufferedReader br = new BufferedReader(new FileReader(file));
			    				
			String line = br.readLine();
			while(line!=null) {
				String[] field = line.split(",");
				map.put(field[0], field[1]+": "+field[4]);
				line = br.readLine();						
			}
			br.close();
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			//splits the the input value on basis of tab and removes white spaces and converts into a string
			String []line = value.toString().trim().split("\t");
			//line[0] has the userID and line[1] has the friend list 
			String userId = line[0];
			if(line.length==2) {
				//mapOutputValue.set(line[1]+ "," + map.get(line[1]));
				//mapOutputValue.set();
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
					for(String data1 : friends)
					{
						if(!data1.equals(data)) {
							String details = map.get(data1);
							mapOutputValue.set(data1+","+details);
							context.write(mapOutputKey, mapOutputValue);
						}
							
					}
					//context.write(mapOutputKey, mapOutputValue);
					
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String friendLists = "";
			HashSet<Text> set = new HashSet<>();
			for(Text val : values) {
				if(set.contains(val)) {
					String [] valueSplit = val.toString().split(",");
					if(friendLists.length()==0) {
						friendLists=valueSplit[1];
					}else {
						friendLists=friendLists+","+valueSplit[1];
					}
				}else {
					set.add(val);
				}				
			}
			context.write(key, new Text("["+friendLists+"]"));
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

		// create a job with name "mutualfriendscity"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "mutualfriendscity");
		job.setJarByClass(MutualFriendsCity.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

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
