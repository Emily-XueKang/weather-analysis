package edu.usfca.cs.mr.bayareaprecipitation;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by xuekang on 11/2/17.
 */
public class BayAreaPrecipitationReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float total = 0;
        for (FloatWritable val : values) {
            total += val.get();
        }
        context.write(new Text(key),new FloatWritable(total));
    }
}



