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
public class ClimateChartReducer extends Reducer<IntWritable, ChartData, IntWritable, ArrayWritable> {
    Map<Integer, Float> htmap = new HashMap<Integer, Float>();//month - high temperature
    Map<Integer, Float> ltmap = new HashMap<Integer, Float>();//month - low temperature
    Map<Integer, Float> ttmap = new HashMap<Integer, Float>();//month - total temperature
    Map<Integer, Integer> cmap = new HashMap<Integer, Integer>();//montn - count
    Map<Integer, Float> pmap = new HashMap<Integer, Float>();//month - total precipitation

    @Override
    protected void reduce(IntWritable key, Iterable<ChartData> values, Context context)
            throws IOException, InterruptedException {
        float highTemperature = 0;
        float lowTemperature = Integer.MAX_VALUE;
        float totalTemperature = 0;
        int tempCount = 0; //count of tempreture records for the month indicated by this key
        float totalRainfall = 0;

        for(ChartData val : values) {
            float temperature = val.getTemperature().get();
            float rainfall = val.getPrecipitation().get();
            //local high
            if(temperature>highTemperature){
                highTemperature = temperature;
            }
            //local low
            if(temperature<lowTemperature){
                lowTemperature = temperature;
            }
            totalTemperature+=temperature;
            totalRainfall+=rainfall;
            tempCount+=1;
        }
        int monthkey = key.get();
        System.out.println("monthkey="+monthkey);
        //update global high
        if(htmap.containsKey(monthkey)){
                if(highTemperature > htmap.get(monthkey)){
                    htmap.put(monthkey,highTemperature);
                }
        }
        else{
            htmap.put(monthkey,highTemperature);
        }
        //update global low
        if(ltmap.containsKey(monthkey)) {
            if (lowTemperature < ltmap.get(monthkey)) {
                ltmap.put(monthkey, lowTemperature);
            }
        }
        else{
            ltmap.put(monthkey, lowTemperature);
        }
        //update global total temp
        if(ttmap.containsKey(monthkey)) {
            float current_tt = totalTemperature + ttmap.get(monthkey);//current total temperature
            ttmap.put(monthkey, current_tt);
        }
        else{
            ttmap.put(monthkey, totalTemperature);
        }
        //update global total rainfal
        if(pmap.containsKey(monthkey)) {
            float current_tr = totalRainfall + pmap.get(monthkey);//current total rainfall
            pmap.put(monthkey, current_tr);
        }
        else{
            pmap.put(monthkey, totalRainfall);
        }
        //update global count of temperature records per month
        if(cmap.containsKey(monthkey)) {
            int current_c = tempCount + cmap.get(monthkey);//current total count
            cmap.put(monthkey, current_c);
        }
        else{
            cmap.put(monthkey, tempCount);
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException{
        //float averageTemperature = 0;
        Writable[] outputs = new Writable[4];
        for(int i=1;i<=12;i++){
            outputs[0] = new FloatWritable(htmap.get(i));//high-temp
            outputs[1] = new FloatWritable(ltmap.get(i));//low-temp
            outputs[2] = new FloatWritable(ttmap.get(i));//total-temp
            outputs[3] = new FloatWritable(pmap.get(i));//total-rain
            ArrayWritable results = new ArrayWritable(FloatWritable.class,outputs);
            context.write(new IntWritable(i),results);
        }
    }
}