package edu.usfca.cs.mr.mystartup;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
/**
 * Created by xuekang on 11/5/17.
 */
public class AmarilloReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float minWind = Integer.MAX_VALUE;
        String bestTime = "";
        for(Text val : values) {
            String[] features = val.toString().split(":");
            String day = features[0];
            String wind = features[1];
            float windSpeed = Float.valueOf(wind);
            if (windSpeed <= minWind) {
                minWind = windSpeed;
                bestTime = day;
            }
        }
        context.write(new Text(bestTime), new Text(String.valueOf(minWind)));
    }
}
