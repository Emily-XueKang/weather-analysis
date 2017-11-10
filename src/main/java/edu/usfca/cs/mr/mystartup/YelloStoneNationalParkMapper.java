package edu.usfca.cs.mr.mystartup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;
import java.util.StringTokenizer;

/**
 * Created by xuekang on 11/5/17.
 */
//geohash of yellow stone -- 9xc
//2.geohash -- 9xc
//7.visibility_surface -- maximum
//9.categorical_freezing_rain_yes1_no0_surface
//13.ategorical_snow_yes1_no0_surface
//29.categorical_rain_yes1_no0_surface
public class YelloStoneNationalParkMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        int i=0;
        String timestamp = "";
        String geohash = "";
        String visibility = "";
        float freezingRain = 0;
        float rain = 0;
        float snow = 0;
        while (itr.hasMoreTokens()) {
            String feature = itr.nextToken();
            if (i == 0) {
                timestamp = feature;
            }
            if (i == 1) {
                geohash = feature;
            }
            if (i == 7) {
                visibility = feature;
            }
            if (i == 9) {
                freezingRain = Float.valueOf(feature);
            }
            if (i == 13) {
                snow = Float.valueOf(feature);
            }
            if (i == 29) {
                rain = Float.valueOf(feature);
            }
            i++;
        }
        if ((geohash.matches("(9xc).*")) && rain==0 && freezingRain==0 && snow==0 ) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(Long.valueOf(timestamp));
            String day = calendar.get( Calendar.YEAR ) + String.format( "%02d", calendar.get( Calendar.MONTH )+1) + calendar.get( Calendar.DATE );
            context.write(new Text(day), new Text(visibility));
        }
    }
}