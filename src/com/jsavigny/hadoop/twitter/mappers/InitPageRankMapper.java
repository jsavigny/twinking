package com.jsavigny.hadoop.twitter.mappers;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitPageRankMapper extends Mapper<Text, Text, Text, Text> {
	private static final DecimalFormat df = new DecimalFormat("0.00000");

	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		Text targetKey = new Text();
		Text targetValue = new Text();
		String[] targets = value.toString().split(",");
		for (String target : targets){
			targetKey.set(target);
			double newPr = 1.00000;
			targetValue.set(df.format(newPr));
			context.write(targetKey, targetValue);
		}

		context.write(key, new Text(value.toString()));

	}

}