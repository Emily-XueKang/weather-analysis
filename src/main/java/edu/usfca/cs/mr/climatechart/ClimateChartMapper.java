package edu.usfca.cs.mr.climatechart;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.StringTokenizer;
import org.joda.time.DateTime;
/**
 * Created by xuekang on 11/1/17.
 */
public class ClimateChartMapper extends Mapper<LongWritable, Text, IntWritable, ChartData> {
    //private final String givenGeoPrefix = "dp6t";//South Bend
    private final String givenGeoPrefix = "9q8y";//bayarea
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr;
        itr = new StringTokenizer(value.toString());
        ArrayList<String> oneRecord = new ArrayList<>();
        while(itr.hasMoreTokens()){
            oneRecord.add(itr.nextToken());
        }
        String timestamp = oneRecord.get(0);
//        long timestamp = Long.valueOf(time);
//        DateTime dt = new DateTime(timestamp);
//        int month = dt.getMonthOfYear();
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.valueOf(timestamp));
        int month = calendar.get(Calendar.MONTH)+1;
        String geo = oneRecord.get(1);
        if(geo.startsWith(givenGeoPrefix)){
            float tempe = Float.valueOf(oneRecord.get(40));
            float rain = Float.valueOf(oneRecord.get(55));
            FloatWritable temperature = new FloatWritable(tempe);
            FloatWritable precipitation = new FloatWritable(rain);
            ChartData cd = new ChartData();
            cd.set(temperature,precipitation);
            context.write(new IntWritable(month), cd);
        }
    }
}
