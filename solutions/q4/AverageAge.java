import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Stack;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Comparator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class AverageAge 
{
	public static class MapperOne extends Mapper<Object, Text, IntWritable, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//Splits on either , or whitespaces
			String [] userId = value.toString().split("[,\\s]");
			for(int i = 1; i < userId.length; i++) 
			{
				//emits in the form (friendId, UserId) for each of file soc-LiveJournal1Adj.txt
				context.write(new IntWritable(Integer.parseInt(userId[i])), new Text("friends,"+ userId[0]));
			}
		}
	}
	
	public static class AgeMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		LocalDate currentDate = LocalDate.now();
		SimpleDateFormat dob = new SimpleDateFormat("MM/dd/yyyy");
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String [] userDetail = value.toString().split(",");
			try {
				//emits (ID,age) (ID, age, 24)
				
				context.write(new IntWritable(Integer.parseInt(userDetail[0])), new Text("age,"+Integer.toString(Period.between(dob.parse(userDetail[9]).toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), currentDate).getYears())));
				} 
			catch (NumberFormatException | ParseException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public static class ReducerOne extends Reducer<IntWritable, Text, IntWritable, Text>
	{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			LinkedList<Integer> list = new LinkedList<>();
			Text age = new Text();
			for(Text val: values) 
			{
				String [] line = val.toString().split(",");
				if(line[0].equals("age")) 
				{
					age = new Text(line[1]);
				}
				else 
				{
					list.add(Integer.parseInt(line[1]));
				}
			}
			for(Integer id:list) 
			{
				context.write(new IntWritable(id), age);
			}
			
		}
	}
	
	//Second Map Reduce Job with Two Mapper's and a Reducer
	public static class MapperTwo extends Mapper<Object, Text, IntWritable, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String [] userDetails = value.toString().split("\\s+");			
			context.write(new IntWritable(Integer.parseInt(userDetails[0])), new Text("age:"+userDetails[1]));
		}
	}
	
	public static class AddressMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String [] userDetails = value.toString().split(",");
			context.write(new IntWritable(Integer.parseInt(userDetails[0])), new Text("address:"+userDetails[3]+", "+userDetails[4]+", "+userDetails[5]));
		}
	}
	
	public static class ReducerTwo extends Reducer<IntWritable, Text, IntWritable, Text>
	{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{	
			int sum = 0;
			int count = 0;
			String address = "";
			for(Text val : values) 
			{
				String [] line = val.toString().split(":");
				if(line[0].equals("address")) 
				{	
					address = line[1];
				}
				else 
				{
					sum = sum + Integer.parseInt(line[1]);
					count ++;
				}
			}
			
			if(count != 0) {
				queue.add(new Node(key.get(),sum/count,address));
			}
		}
		
		public class Node
		{	
			int userId;
			int age;
			String address;
			
			public Node(int ui, int ag, String ad) 
			{	
				userId = ui;
				age = ag;
				address = ad;
			}
		}
		private PriorityQueue<Node> queue = new PriorityQueue<>(new NodeComparator());
		private Stack<Node> stack = new Stack<>();
		
		public static class NodeComparator implements Comparator<Node>{
			public int compare(Node left, Node right) {
		        //ascending order
				return left.age - right.age;
		    }
		}
	    	    
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			int count = 0;
			while(count != 15) 
			{
				stack.add(queue.poll());
				count++;
			}
			while(!stack.isEmpty()) 
			{
				Node tmp = stack.pop();
				context.write(new IntWritable(tmp.userId), new Text(tmp.address+", "+Integer.toString(tmp.age)));
			}
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job1 = new Job(conf, "AverageAgeOfFriends");
		job1.setJarByClass(AverageAge.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setReducerClass(ReducerOne.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, MapperOne.class);
		MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class, AgeMapper.class);
		FileOutputFormat.setOutputPath(job1,  new Path(otherArgs[2]+"_temp"));
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf, "BottomFifteenAverageAge");
		job2.setJarByClass(AverageAge.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setNumReduceTasks(1);
		job2.setReducerClass(ReducerTwo.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job2, new Path(otherArgs[2]+"_temp"), TextInputFormat.class, MapperTwo.class);
		MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, AddressMapper.class);
		FileOutputFormat.setOutputPath(job2,  new Path(otherArgs[2]));
		System.exit(job2.waitForCompletion(true)?0:1);
	}
}
