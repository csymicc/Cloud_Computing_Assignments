package wordcount.wc1;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount3 {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {  
		private final static IntWritable one = new IntWritable(1);  
		private HashSet<String> dict = new HashSet<String>();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		    if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
		    	File file = new File("./dict");
		    	Scanner sc = new Scanner(new File(file.getPath()));
	    		while(sc.hasNextLine()) {
	    			String line = sc.nextLine();
	    			String[] words = line.split(" ");
	    			for(String w : words) {
	    				dict.add(w);
	    			}
	    		}
	    		sc.close();
		    }
		    super.setup(context);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
			String line = value.toString();
		    StringTokenizer tokenizer = new StringTokenizer(line);
		    while (tokenizer.hasMoreTokens()) {
		    	String tokens = tokenizer.nextToken();
		    	if(dict.contains(tokens)) {
		    		context.write(new Text(tokens), one);  
		    	}
		    }
		}  
	}  
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {  
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {  
		    int sum = 0;  
		    for(IntWritable value : values) {
		        sum += value.get();  
		    }  
		    context.write(key, new IntWritable(sum));  
		}
	}  
	
	public static void main(String[] args) throws Exception {  
		Job job = Job.getInstance();
		
		job.addCacheFile(new URI("/home/ec2-user/Dict/word-patterns.txt#dict"));
		job.setJarByClass(WordCount3.class);
		job.setJobName("WordCount3");
		job.setOutputKeyClass(Text.class);  
		job.setOutputValueClass(IntWritable.class);  
		job.setMapperClass(Map.class);  
		job.setReducerClass(Reduce.class); 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);  
		FileInputFormat.setInputPaths(job, new Path(args[0]));  
		FileOutputFormat.setOutputPath(job, new Path(args[1]));  
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}  
}
