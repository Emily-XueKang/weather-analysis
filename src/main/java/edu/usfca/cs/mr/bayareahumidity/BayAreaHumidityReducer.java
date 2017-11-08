package edu.usfca.cs.mr.bayareahumidity;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by xuekang on 11/2/17.
 */
public class BayAreaHumidityReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float total = 0;
        float average = 0;
        int count = 0;
        for (FloatWritable val : values) {
            total += val.get();
            count+=1;
        }
        average = total/count;
        context.write(new Text(key),new FloatWritable(average));
    }
}



