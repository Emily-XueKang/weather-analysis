package edu.usfca.cs.mr.powercompany;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xuekang on 11/7/17.
 */
public class SolarWindReducer extends Reducer<Text, SolarWind, Text, SolarWind>{
    @Override
    protected void reduce(Text key, Iterable<SolarWind> values, Context context) throws IOException, InterruptedException {
        double totalWindspeed = 0.0;
        double totalCloud = 0.0;
        int count = 0;
        double averageWindspeed = 0.0;
        double averageCould = 0.0;
        for(SolarWind val : values){
            Text cloud = val.getCloudcover();
            Text windSpeed = val.getWindgust();
            totalWindspeed +=  Double.valueOf(windSpeed.toString());
            totalCloud += Double.valueOf(cloud.toString());
            count++;
        }
        averageCould = totalCloud/count;
        averageWindspeed = totalWindspeed/count;
        if (averageWindspeed>10 && averageCould<50 && averageCould>=0.0) {
            context.write(key, new SolarWind(new Text(String.valueOf(averageWindspeed)), new Text(String.valueOf(averageCould))));
        }
    }
}
