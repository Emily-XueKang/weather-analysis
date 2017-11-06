package edu.usfca.cs.mr.mystartup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by xuekang on 11/4/17.
 */
public class CapeTownReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean lesscloud = false;
        boolean lesspricipitation = false;
        int i = 0;
        String bestDate = "";
        for(Text val : values) {
            StringTokenizer itr = new StringTokenizer(val.toString());
            String yearmonth = "";
            String cloud = "";
            String pricipitation = "";
            while (itr.hasMoreTokens()) {
                String feature = itr.nextToken();
                if (i == 0) {
                    yearmonth = feature;
                }
                if (i == 1) {
                    cloud = feature;
                }
                if (i == 2) {
                    pricipitation = feature;
                }
                i++;
            }
            if (Float.valueOf(cloud) <= 50 && Float.valueOf(pricipitation)<20) {
                lesscloud = true;
                lesspricipitation = true;
                bestDate = yearmonth;
            }
        }
        if(lesscloud==true && lesspricipitation==true)
        context.write(key, new Text(bestDate));
    }
}
