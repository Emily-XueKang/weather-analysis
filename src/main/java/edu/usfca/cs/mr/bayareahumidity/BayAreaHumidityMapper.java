package edu.usfca.cs.mr.bayareahumidity;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
/**
 * Created by xuekang on 11/2/17.
 */
public class BayAreaHumidityMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    //bay area geo prefix: 9qb/c/8/9
    //13:relative_humidity_zerodegc_isotherm
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr;
        itr = new StringTokenizer(value.toString());
        ArrayList<String> oneRecord = new ArrayList<>();
        while(itr.hasMoreTokens()){
            oneRecord.add(itr.nextToken());
        }
        String geohash = oneRecord.get(1);
        float precipitation = Float.valueOf(oneRecord.get(12));
        if(geohash.substring(0,3).equals("9qb")||geohash.substring(0,3).equals("9qc")||geohash.substring(0,3).equals("9q8")||geohash.substring(0,3).equals("9q9")){
            context.write(new Text(geohash),new FloatWritable(precipitation));
        }
    }
}
