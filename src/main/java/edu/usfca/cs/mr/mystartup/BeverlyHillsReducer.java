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
public class BeverlyHillsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean lesscloud = false;
        boolean lesspricipitation = false;
        String bestDate = "";
        for(Text val : values) {
            String cloud = "";
            String pricipitation = "";
            String value = val.toString();
            String[] features = value.split( ":" );
            cloud = features[0];
            pricipitation = features[1];
            if (Float.valueOf(cloud) <= 50 && Float.valueOf(pricipitation)<20) {
                lesscloud = true;
                lesspricipitation = true;
            }else{
                lesscloud = false;
                lesspricipitation = false;
            }
        }
        if(lesscloud==true && lesspricipitation==true)
        context.write(key, new Text(bestDate));
    }
}
