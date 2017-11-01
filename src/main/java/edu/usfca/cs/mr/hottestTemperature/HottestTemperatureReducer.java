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
public class HottestTemperatureReducer extends Reducer<Text, Text, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float hightes = 0;
        String hightimegeo="";
        for(Text val:values){
            String[] items = val.toString().split("-");
            float temperature = Float.valueOf(items[1]);
            if(temperature>hightes){
                hightes=temperature;
                hightimegeo = items[0];
            }
        }
        context.write(new Text(hightimegeo),new FloatWritable(hightes));
    }
//    @Override
//    protected void cleanup(Context context) throws IOException,InterruptedException{
//        float high = 0;
//        String highgeotime = "";
//        for(Text geotime:temperatrueMap.keySet()){
//            if(temperatrueMap.get(geotime)>high) {
//                high = temperatrueMap.get(geotime);
//                highgeotime = geotime.toString();
//            }
//        }
//        context.write(new Text(highgeotime), new FloatWritable(high));
//    }
}