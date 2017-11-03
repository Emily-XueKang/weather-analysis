package edu.usfca.cs.mr.climatechart;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.joda.time.DateTime;
/**
 * Created by xuekang on 11/1/17.
 */
public class ClimateChartMapper extends Mapper<LongWritable, Text, IntWritable, ChartData> {
    private final String givenGeoPrefix = "wqj";//Xi'an, Shaanxi, China
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr;
        itr = new StringTokenizer(value.toString());
        ArrayList<String> oneRecord = new ArrayList<>();
        while(itr.hasMoreTokens()){
            oneRecord.add(itr.nextToken());
        }
        String time = oneRecord.get(0);
        long timestamp = Long.valueOf(time);
        DateTime dt = new DateTime(timestamp);
        int month = dt.getMonthOfYear();
        String geo = oneRecord.get(1);
        float tempe = Float.valueOf(oneRecord.get(40));
        float rain = Float.valueOf(oneRecord.get(55));
        FloatWritable temperature = new FloatWritable(tempe);
        FloatWritable precipitation = new FloatWritable(rain);
        if(geo.startsWith(givenGeoPrefix)){
            context.write(new IntWritable(month), new ChartData(temperature,precipitation));
        }
    }
}
