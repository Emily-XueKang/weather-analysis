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
        double totalwindSpeed = 0.0;
        double totalCloud = 0.0;
        Text geohash = null;
        for(SolarWind val : values){
            geohash = new Text(val.getGeohash());
            Text cloud = val.getCloudCover();
            Text windSpeed = val.getWindGust();
            totalwindSpeed +=  Double.valueOf(windSpeed.toString());
            totalCloud += Double.valueOf(cloud.toString());
        }

        if (totalwindSpeed > 60 && totalCloud < 100 &&totalCloud >= 0.0 ) {
            context.write(key, new SolarWind(new Text(String.valueOf(totalwindSpeed)), new Text(String.valueOf(totalCloud)), geohash));
        }
    }
}
