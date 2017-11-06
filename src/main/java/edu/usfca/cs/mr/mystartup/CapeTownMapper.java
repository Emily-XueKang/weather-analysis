package edu.usfca.cs.mr.mystartup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;
import java.util.StringTokenizer;
/**
 * Created by xuekang on 11/4/17.
 */

//17.total_cloud_cover_entire_atmosphere -- float, below 50(%)
//41.temperature_surface -- 298K(25 C) to 313K(40 C)
//56.precipitable_water_entire_atmosphere -- float, below 20.00
//2.geohash -- k3vp,k3vn,k3vj,k3vh
public class CapeTownMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        int i=0;
        String timestamp = "";
        String geohash = "";
        String temp = "";
        String cloud = "";
        String precipitation = "";
        while (itr.hasMoreTokens()) {
            String feature = itr.nextToken();

            if (i == 0) {
                timestamp = feature;
            }
            if (i == 1) {
                geohash = feature;
            }
            if (i == 16) {
                cloud = feature;
            }
            if (i == 40) {
                temp = feature;
            }
            if (i == 55) {
                precipitation = feature;
            }
            i++;
        }
        if ((Float.valueOf(temp) > 298) && (Float.valueOf(temp) < 313) && (geohash.matches("(k3vp|k3vn|k3vj|k3vh).*"))) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(Long.valueOf(timestamp));
            String yearmonth = calendar.get(Calendar.YEAR) + String.format("%02d", calendar.get(Calendar.MONTH)+1) ;
            context.write(new Text("CapeTown Best Times"), new Text(yearmonth + "\t " + cloud + "\t" + precipitation));
        }
    }
}
