package edu.usfca.cs.mr.hottestTemperature;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Created by xuekang on 10/29/17.
 */
//public class HottestTemperatureMapper extends Mapper<LongWritable, Text, TimeGeohash, FloatWritable> {
//    @Override
//    protected void map(LongWritable key, Text value, Context context)
//            throws IOException, InterruptedException {
//        StringTokenizer itr;
//        itr = new StringTokenizer(value.toString());
//        ArrayList<String> oneRecord = new ArrayList<>();
//        while(itr.hasMoreTokens()){
//            oneRecord.add(itr.nextToken());
//        }
//        //float time;
//        //time = Long.valueOf(oneRecord.get(0));
//        float temperature;
//        Text time = new Text(oneRecord.get(0));
//        Text geohash = new Text(oneRecord.get(1)); //Geohash
//        //TimeGeohash tg = new TimeGeohash(new Text(time), new Text(geohash));
//        TimeGeohash tg = new TimeGeohash();
//        tg.set(time, geohash);
//        temperature = Float.valueOf(oneRecord.get(40)); //temperature_surface
//        context.write(tg, new FloatWritable(temperature));
//    }
//}
public class HottestTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr;
        itr = new StringTokenizer(value.toString());
        ArrayList<String> oneRecord = new ArrayList<>();
        while(itr.hasMoreTokens()){
            oneRecord.add(itr.nextToken());
        }

        float temperature;
        String time = oneRecord.get(0);
        String geo = oneRecord.get(1);
        temperature = Float.valueOf(oneRecord.get(40)); //temperature_surface
        context.write(new Text("highestTemperature"), new Text(time+":"+geo+"-"+temperature));
    }
}
