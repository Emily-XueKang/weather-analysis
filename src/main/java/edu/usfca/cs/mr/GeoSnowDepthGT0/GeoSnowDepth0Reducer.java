package edu.usfca.cs.mr.GeoSnowDepthGT0;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Created by xuekang on 10/27/17.
 */
public class GeoSnowDepth0Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        boolean hasZero = false;
        double average = 0.0;
        double total = 0.0;
        int count = 0;
        for(DoubleWritable val:values) {
            if (val.get() == 0.0) {
                hasZero = true;
            }
            total+=val.get();
            count++;
        }
        average = total/count;
        if(hasZero==false){
            context.write(new Text(key),new DoubleWritable(average));
        }
    }
}