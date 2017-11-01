package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Created by xuekang on 10/31/17.
 */
public class LighteningMapper extends Mapper<LongWritable, Text, Text, FloatWritable>  {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr;
        itr = new StringTokenizer(value.toString());
        ArrayList<String> oneRecord = new ArrayList<>();
        while(itr.hasMoreTokens()){
            oneRecord.add(itr.nextToken());
        }

        String geo = oneRecord.get(1).substring(0, 4);
        float lightening = Float.valueOf(oneRecord.get(22)); //lightening_surface
        if (lightening != 0.0) {
            context.write(new Text(geo), new FloatWritable(lightening));
        }
    }
}
