package edu.usfca.cs.mr.hottestTemperature;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by xuekang on 10/29/17.
 */
public class HottestTemperatureReducer extends Reducer<Text, ArrayList<String>, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<ArrayList<String>> values, Context context) throws IOException, InterruptedException {
        float high = 0;
        String highgeotime = "";
        for(ArrayList<String> val:values){
            String temperaturestring = val.get(2);
            float temperature = Float.valueOf(temperaturestring);

            if(temperature>high){
                high = temperature;
                highgeotime = val.get(0)+" : "+val.get(1);
            }
        }
        context.write(new Text("high"),new Text(highgeotime + " has "+high));
    }
}
