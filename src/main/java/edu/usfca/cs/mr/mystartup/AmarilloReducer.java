package edu.usfca.cs.mr.mystartup;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xuekang on 11/5/17.
 */
public class AmarilloReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float minWind = Integer.MAX_VALUE;
        String bestTime = "";
        String day = "";
        String wind = "";
        float windSpeed = 0;
        for(Text val : values) {
            String[] features = val.toString().split(":");
            System.out.println("featureslength=======" + features.length);
            for(int i=0;i<features.length;i++){
                System.out.println("f["+i+"]====="+features[i]);
            }
            day = features[0];
            wind = features[1];
            windSpeed = Float.valueOf(wind);
            if (windSpeed <= minWind) {
                minWind = windSpeed;
                bestTime = day;
            }
            context.write(new Text(bestTime), new Text(String.valueOf(minWind)));
        }
    }
}
