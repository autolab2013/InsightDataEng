import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


public class PageRank {

	public static final long precision = 1000000;

	public static void main(String[] args) throws IOException {
		int numRepititions = 5;
		long leftover = 0;
		long size = 0;
		for(int i = 0; i < 2*numRepititions; i++) {
			Job job;
			if(i%2 == 0) {
				job = getTrustJob();
			}
			else {
				job = getLeftoverJob(leftover, size);
			}

			String inputPath = i == 0 ? "input" : "stage" + (i-1);
			String outputPath = "stage" + i;

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));

			try {
				job.waitForCompletion(true);
			} catch(Exception e) {
				System.err.println("ERROR IN JOB: " + e);
				return;
			}

			//get Hadoop Counters
			Counters counters = job.getCounters();
			Counter size_cnt = counters.findCounter(HadoopCounter.SIZE);
			size = size_cnt.getValue();
			Counter leftover_cnt = counters.findCounter(HadoopCounter.LEFTOVER);
			System.out.println("Page Rank leftover value: "+ leftover_cnt.getValue());

			if(i%2 == 0) {//Trust Job done
				// Set up leftover and size
				leftover = leftover_cnt.getValue();
			} else {//Leftover Job done
				// Set up leftover and size
				leftover_cnt.setValue(0);//clear leftover
			}
		}
	}
	public static Job getStandardJob(String l, String s) throws IOException {
		Configuration conf = new Configuration();
		if(!l.equals("") && !s.equals("")) {
			conf.set("leftover", l);
			conf.set("size", s);
		}
		Job job = new Job(conf);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Node.class);

		job.setInputFormatClass(NodeInputFormat.class);
		job.setOutputFormatClass(NodeOutputFormat.class);

		job.setJarByClass(PageRank.class);

		return job;
	}

	public static Job getTrustJob() throws IOException{

		Job job = getStandardJob("", "");

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NodeOrDouble.class);

		job.setMapperClass(TrustMapper.class);
		job.setReducerClass(TrustReducer.class);

		return job;
	}

	public static Job getLeftoverJob(long l, long s) throws IOException{
		Job job = getStandardJob("" + l, "" + s);

		job.setMapperClass(LeftoverMapper.class);
		job.setReducerClass(LeftoverReducer.class);

		return job;
	}
}
	       

    


