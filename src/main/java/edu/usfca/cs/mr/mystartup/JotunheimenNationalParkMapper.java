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
//2.geohash -- u4vy,u4yn,u4vv,u4yj
//42.snow_cover_surface -- float 100
//7. visibility_surface -- maximum
public class JotunheimenNationalParkMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        int i=0;
        String timestamp = "";
        String geohash = "";
        String visibility = "";
        String snowCover = "";
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
            if (i == 41) {
                snowCover = feature;
            }
            i++;
        }
        if (snowCover.equals("100") && (geohash.matches("(u4vy|u4yn|u4vv|u4yj).*"))) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(Long.valueOf(timestamp));
            String yearmonth = calendar.get(Calendar.YEAR) + String.format("%02d", calendar.get(Calendar.MONTH)+1) ;
            context.write(new Text("Jotunheimen National Park Best Times"), new Text(yearmonth + "\t " + visibility));
        }
    }
}