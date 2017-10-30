package edu.usfca.cs.mr.GeoSnowDepthGT0;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Created by xuekang on 10/27/17.
 */
public class GeoSnowDepth0Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        for(FloatWritable val:values){
            if(val.get()>0.0){
                context.write(new Text(key),new FloatWritable(val.get()));
            }
        }
    }
}
