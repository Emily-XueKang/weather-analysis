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
//    @Override
//    protected void reduce(Text key, Iterable<SolarWind> values, Context context) throws IOException, InterruptedException {
//        BigDecimal totalWind = new BigDecimal(0.0);
//        BigDecimal totalCloud = new BigDecimal(0.0);
//        int count = 0;
//        double averageWindspeed = 0.0;
//        double averageCould = 0.0;
//        for(SolarWind val : values){
//            double cloud = val.getCloudcover().get()/100;
//            double wind = val.getWindgust().get();
//            totalWind = totalWind.add(new BigDecimal(wind));
//            totalCloud = totalCloud.add(new BigDecimal(cloud));
//            count++;
//        }
//        averageCould = totalCloud.divide(new BigDecimal(count),2, RoundingMode.HALF_UP).doubleValue();
//        averageWindspeed = totalWind.divide(new BigDecimal(count),2, RoundingMode.HALF_UP).doubleValue();
//        if (averageWindspeed>10 && averageCould<50) {
//            context.write(key, new SolarWind(new DoubleWritable(averageWindspeed), new DoubleWritable(averageCould)));
//        }
//    }
    @Override
    protected void reduce(Text key, Iterable<SolarWind> values, Context context) throws IOException, InterruptedException {
        double totalWind = 0.0;
        double totalCloud = 0.0;
        int count = 0;
        double averageWind = 0.0;
        double averageCould = 0.0;
        for(SolarWind val : values){
            double cloud = val.getCloudcover().get()/100;
            double wind = val.getWindgust().get();
            totalWind += wind;
            totalCloud += cloud;
            count++;
        }
        averageCould = totalCloud/count;
        averageWind = totalWind/count;
        if (averageWind>10 && averageCould<0.5) {
            context.write(key, new SolarWind(new DoubleWritable(averageWind), new DoubleWritable(averageCould)));
        }
    }
}
