package edu.usfca.cs.mr.mystartup;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;
import java.util.StringTokenizer;
/**
 * Created by xuekang on 11/7/17.
 */
//1.geohash:dhwg|dhx5|dhwf|dhx4|dhwc|dhx1
//56.precipitable_water_entire_atmosphere < 25
//23.lightning_surface -- 0
public class MiamiMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer( line );
        int i=0;
        String timestamp = "";
        String geohash = "";
        String precipitatoin = "";
        String lighting_surface = "";
        while (itr.hasMoreTokens()) {
            String item = itr.nextToken();
            if (i == 0) timestamp = item;
            if (i == 1) geohash = item;
            if (i == 23) lighting_surface = item;
            if (i == 55) precipitatoin = item;
            i++;
        }
        Float light = Float.valueOf(lighting_surface);
        if (geohash.matches("(dhwg|dhx5|dhwf|dhx4|dhwc|dhx1).*") && (light==0)) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis( Long.valueOf( timestamp ) );
            String day = calendar.get( Calendar.YEAR ) + String.format( "%02d", calendar.get( Calendar.MONTH )+1) + calendar.get( Calendar.DATE ) ;
            context.write(new Text(day), new Text(precipitatoin));
        }
    }
}
