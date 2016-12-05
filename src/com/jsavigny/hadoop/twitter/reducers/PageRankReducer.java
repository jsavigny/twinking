package com.jsavigny.hadoop.twitter.reducers;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    private static final DecimalFormat df = new DecimalFormat("0.00000");

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {


        Text targetKey = new Text();
        Text targetValue = new Text();

        String outlink_list = "null";
        double pr = 0;
        for (Text v : values){
            String[] targets = v.toString().split(",");
            if (isDecimal(v.toString())){
                pr += Double.parseDouble(v.toString());
            } else if (!targets[0].equals("null")) {
                outlink_list = v.toString();
            }

        }
        pr = 1 - 0.85 + (0.85 * pr);
        targetKey.set(key + "," + df.format(pr));
        targetValue.set(outlink_list);
        context.write(targetKey, targetValue);

	}
    private boolean isDecimal(String s){
        String re = "[0-9]+\\.[0-9]{5}";
        return s.matches(re);
    }

}