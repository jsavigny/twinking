package com.jsavigny.hadoop.twitter.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FollowingReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		boolean empty = true;
		for (Text value : values){
			if (!empty) {
				sb.append(",");
			}
			empty = false;
			sb.append(value.toString());
		}
		context.write(key, new Text(sb.toString()));

	}

}