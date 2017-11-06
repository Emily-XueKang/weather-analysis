package edu.usfca.cs.mr.mystartup;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
/**
 * Created by xuekang on 11/5/17.
 */
public class WellingtonReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double minWind = Integer.MAX_VALUE;
        String bestTime = "";
        for(Text val : values) {
            String[] features = val.toString().split("\t");
            String yearmonth = features[0];
            String vOfWind = features[1];
            String uOfWind = features[2];
            double wind = Math.sqrt(Math.pow(Double.valueOf(uOfWind), 2) + Math.pow(Double.valueOf(vOfWind), 2));
            if (wind <= minWind) {
                minWind = wind;
                bestTime = yearmonth;
            }
        }
        context.write(key, new Text(String.valueOf(bestTime)));
    }
}
