package com.jsavigny.hadoop.twitter.reducers;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalizeReducer extends Reducer<DoubleWritable, IntWritable, IntWritable, DoubleWritable> {

    public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        for (IntWritable value : values){
            context.write(value, key);
        }

    }

}