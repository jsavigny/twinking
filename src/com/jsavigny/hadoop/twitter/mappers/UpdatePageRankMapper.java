package com.jsavigny.hadoop.twitter.mappers;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UpdatePageRankMapper extends Mapper<Text, Text, Text, Text> {

	private static final DecimalFormat df = new DecimalFormat("0.00000");
	
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

        Text targetKey = new Text();
        Text targetValue = new Text();
        String[] keys = key.toString().split(",");
        String keyId = keys[0];;
        double pr = Double.parseDouble(keys[1]);

        String[] targets = value.toString().split(",");
        for (String target : targets){
            if (!target.equals("null")) {
                targetKey.set(target);
                double newPr = pr / targets.length;
                targetValue.set(df.format(newPr));
                context.write(targetKey, targetValue);
            }
        }

        targetKey.set(keyId);
        targetValue.set(value);
        context.write(targetKey,targetValue);
	}

}