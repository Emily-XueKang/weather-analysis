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
public class HottestTemperatureMapper extends Mapper<LongWritable, Text, TimeGeohash, FloatWritable> {
    //timestamp--double, geohash--text, temperature--float
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr;
        itr = new StringTokenizer(value.toString());
        ArrayList<String> oneRecord = new ArrayList<>();
        while(itr.hasMoreTokens()){
            oneRecord.add(itr.nextToken());
        }
        //float time;
        //time = Long.valueOf(oneRecord.get(0));
        String time;
        String Geohash;
        float temperature;
        time = oneRecord.get(0);
        Geohash = oneRecord.get(1); //Geohash
        TimeGeohash tg = new TimeGeohash(new Text(time), new Text(Geohash));
        temperature = Float.valueOf(oneRecord.get(40)); //temperature_surface
        context.write(tg, new FloatWritable(temperature));
    }
}
