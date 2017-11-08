package edu.usfca.cs.mr.climatechart;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xuekang on 11/1/17.
 */
public class ClimateChartReducer extends Reducer<IntWritable, ChartData, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<ChartData> values, Context context)
            throws IOException, InterruptedException {
        float highTemperature = 0;
        float lowTemperature = Integer.MAX_VALUE;
        float totalTemperature = 0;
        int count = 0; //count of tempreture records for the month indicated by this key
        float totalRainfall = 0;
        float average_temp = 0;
        float average_rain = 0;
        for(ChartData val : values) {
            float temperature = val.getTemperature().get();
            float rainfall = val.getPrecipitation().get();
            if(temperature>highTemperature){
                highTemperature = temperature;
            }
            if(temperature<lowTemperature){
                lowTemperature = temperature;
            }
            totalTemperature+=temperature;
            totalRainfall+=rainfall;
            count+=1;
        }
        average_temp = totalTemperature/count;
        average_rain = totalRainfall/count;
        int monthkey = key.get();
        //<month-num>  <high-temp>  <low-temp>  <average-precip>  <avg-temp>
        String results = highTemperature+"\t"+lowTemperature+"\t"+average_rain+"\t"+average_temp;
        context.write(new IntWritable(monthkey),new Text(results));
    }
}