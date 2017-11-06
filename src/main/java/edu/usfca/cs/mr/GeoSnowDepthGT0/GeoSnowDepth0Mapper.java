package edu.usfca.cs.mr.GeoSnowDepthGT0;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Created by xuekang on 10/27/17.
 */
public class GeoSnowDepth0Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr;
        itr = new StringTokenizer(value.toString());
        ArrayList<String> oneRecord = new ArrayList<>();
        while(itr.hasMoreTokens()){
            oneRecord.add(itr.nextToken());
        }
        String geohash;
        double snowDepth;
        geohash = oneRecord.get(1); //Geohash
        snowDepth = Double.valueOf(oneRecord.get(50)); //snow_depth_surface
        context.write(new Text(geohash), new DoubleWritable(snowDepth));
    }
}
