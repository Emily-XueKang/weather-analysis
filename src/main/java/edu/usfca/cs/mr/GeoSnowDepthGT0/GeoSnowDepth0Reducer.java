package edu.usfca.cs.mr.GeoSnowDepthGT0;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Created by xuekang on 10/27/17.
 */
public class GeoSnowDepth0Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for(DoubleWritable val:values){
            if(val.get()>0.0){
                context.write(new Text(key),new DoubleWritable(val.get()));
            }
        }
    }
}
