package edu.usfca.cs.mr.hottestTemperature;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xuekang on 10/29/17.
 */
public class HottestTemperatureReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float high = 0;
        String highgeotime = "";
        for(FloatWritable val:values){
            if(val.get()>high){
                high = val.get();
                highgeotime = key.toString();
            }
        }
        context.write(new Text(highgeotime),new FloatWritable(high));
    }
}
