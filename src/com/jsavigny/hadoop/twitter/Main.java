package com.jsavigny.hadoop.twitter;


import com.jsavigny.hadoop.twitter.mappers.FinalizeMapper;
import com.jsavigny.hadoop.twitter.mappers.InitPageRankMapper;
import com.jsavigny.hadoop.twitter.mappers.UpdatePageRankMapper;
import com.jsavigny.hadoop.twitter.reducers.FollowingReducer;
import com.jsavigny.hadoop.twitter.reducers.PageRankReducer;
import com.jsavigny.hadoop.twitter.reducers.FinalizeReducer;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Main {
    private static 	Configuration conf = new Configuration();
    private static String dir = "/user/jsavigny/";
    private static String input = "/user/twitter/";
    private static int iteration = 3;


    public static void main(String[] args) throws Exception {
		
		StopWatch timer = new StopWatch();
		timer.start();

		clean();

		String[] prepareOpts = { input + "tes", dir + "prepared.out" };
        prepareTwitterData(prepareOpts);

		String[] initOpts = { dir + "prepared.out", dir + "pr-0.out" };
        initPageRank(initOpts);

		for(int i = 1; i < 3; i++){
			String previous = dir + "pr-" + (i - 1) + ".out";
			String current = dir + "pr-" + i + ".out";
			String[] opts = {previous, current};
            updatePageRank(opts);
		}

		String[] finalizeOpts = { dir + "pr-2.out", dir + "finalized.out" };
		finalize(finalizeOpts);

		timer.stop();
		System.out.println("Elapsed " + timer.toString());
	}

	public static void clean() throws  Exception{
	    FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(dir + "prepared.out"))){
            fs.delete(new Path(dir + "prepared.out"), true);
        }

        for(int i = 0; i < 3; i++){
            if (fs.exists(new Path(dir + "pr-" + i + ".out"))){
                fs.delete(new Path(dir + "pr-" + i + ".out"), true);
            }
        }
        if (fs.exists(new Path(dir + "finalized.out"))){
            fs.delete(new Path(dir + "finalized.out"), true);
        }
    }

    public static int initPageRank(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "[jsavigny] init page rank");
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJarByClass(Main.class);
        job.setMapperClass(InitPageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int prepareTwitterData(String[] args) throws Exception {
        System.out.println("prepare data");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "[jsavigny] prepare twitter data");
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJarByClass(Main.class);
        job.setMapperClass(InverseMapper.class);
        job.setCombinerClass(FollowingReducer.class);
        job.setReducerClass(FollowingReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static int updatePageRank(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "[jsavigny] update page rank");
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJarByClass(Main.class);
        job.setMapperClass(UpdatePageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int finalize(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "[jsavigny] finalize page rank");
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJarByClass(Main.class);
        job.setMapperClass(FinalizeMapper.class);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job.setReducerClass(FinalizeReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
