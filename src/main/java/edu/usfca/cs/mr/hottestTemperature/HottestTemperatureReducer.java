package edu.usfca.cs.mr.hottestTemperature;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xuekang on 10/29/17.
 */
public class HottestTemperatureReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private Map<String, Float> temperatrueMap = new HashMap<>();
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        float temperature = 0;
        for(FloatWritable val:values){
            temperature = val.get();
        }
        temperatrueMap.put(key.toString(),temperature);
    }
    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException{
        //FloatWritable high= new FloatWritable(0);
        float high = 0;
        String highgeotime = "";
        for(String geotime:temperatrueMap.keySet()){
            if(temperatrueMap.get(geotime)>high) {
                high = temperatrueMap.get(geotime);
                highgeotime = geotime;
            }
        }
        context.write(new Text(highgeotime), new FloatWritable(high));
    }
}