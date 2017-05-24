package uber.tracking;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Trip_tracking extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new Trip_tracking(), args);
		System.exit(exitcode);
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

	//	if(args.length!=2)
	//	{
	//		System.out.println("Usage: [input] [output]");
	//		System.exit(-1);
	//	}
		
		Job job = Job.getInstance(getConf(), "Uber Trip tracking to fiund the more trips for each basement");
		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path("In"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "23/05/17"));

		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(Sum_reducer.class);
		job.setReducerClass(Sum_reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 :1;
	}

}
