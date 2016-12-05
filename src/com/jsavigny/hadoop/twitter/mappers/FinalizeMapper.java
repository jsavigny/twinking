package com.jsavigny.hadoop.twitter.mappers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

    public class FinalizeMapper extends Mapper<Text, Text, DoubleWritable, IntWritable> {

    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] keys = key.toString().split(",");
        String userID = keys[0];
        String pageRank = keys[1];
        context.write(new DoubleWritable(Double.valueOf(pageRank)), new IntWritable(Integer.valueOf(userID)));
    }

}