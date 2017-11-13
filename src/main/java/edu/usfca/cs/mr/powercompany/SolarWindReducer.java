package edu.usfca.cs.mr.powercompany;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Created by xuekang on 11/7/17.
 */
public class SolarWindReducer extends Reducer<Text, SolarWind, Text, SolarWind>{
    @Override
    protected void reduce(Text key, Iterable<SolarWind> values, Context context) throws IOException, InterruptedException {
        //double totalWind = 0.0;
        BigDecimal totalWind = new BigDecimal(0.0);
        BigDecimal totalCloud = new BigDecimal(0.0);
        int count = 0;
        double averageWindspeed = 0.0;
        double averageCould = 0.0;
        for(SolarWind val : values){
            DoubleWritable cloud = val.getCloudcover();
            DoubleWritable wind = val.getWindgust();
            //totalWind +=  wind.get();
            totalWind = totalWind.add(new BigDecimal(wind.get()));
            totalCloud = totalCloud.add(new BigDecimal(cloud.get()));
            count++;
        }
        averageCould = totalCloud.divide(new BigDecimal(count),2, RoundingMode.HALF_UP).doubleValue();
        averageWindspeed = totalWind.divide(new BigDecimal(count)).doubleValue();
        if (averageWindspeed>10 || averageCould<50) {
            context.write(key, new SolarWind(new DoubleWritable(averageWindspeed), new DoubleWritable(averageCould)));
        }
    }
}
