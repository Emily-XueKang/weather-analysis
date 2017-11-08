package edu.usfca.cs.mr.powercompany;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
/**
 * Created by xuekang on 11/7/17.
 */
//19.land_cover_land1_sea0_surface
public class SolarWindMapper extends Mapper<LongWritable, Text, Text, SolarWind> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer( line );
        int i=0;
        String wind_gust = "";
        String cloud_cover = "";
        String geohash = "";
        String land = "";
        while (itr.hasMoreTokens()) {
            String feature = itr.nextToken();
            if (i == 1) geohash = feature;
            if (i == 15) wind_gust = feature;
            if (i == 16) cloud_cover = feature;
            if(i==18) land = feature;
            i++;
        }
        if(land == "1"){
            context.write(new Text(geohash.substring(0,5)), new SolarWind(new Text(wind_gust), new Text(cloud_cover), new Text(geohash)));
        }
    }
}
