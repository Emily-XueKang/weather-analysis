package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xuekang on 10/31/17.
 */
public class LighteningReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float count = 0;
        // calculate the total count of lightenings per geohash
        for (FloatWritable val : values) {
            count += val.get();
        }
        context.write(key,new FloatWritable(count));
    }
}


