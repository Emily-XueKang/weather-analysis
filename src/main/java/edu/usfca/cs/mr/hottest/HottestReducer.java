package edu.usfca.cs.mr.hottest;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xuekang on 10/29/17.
 */
public class HottestReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float high = 0;
        for(FloatWritable val:values){
            if(val.get()>high){
                high = val.get();
                context.write(new Text(key),new FloatWritable(high));
            }
        }
    }
}
