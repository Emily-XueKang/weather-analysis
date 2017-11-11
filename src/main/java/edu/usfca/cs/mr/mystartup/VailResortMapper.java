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
//2.Geohash;
//8.visibility_surface
//42.snow_cover_surface -- 100(%)
public class VailResortMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer( line );
        int i=0;
        String timestamp = "";
        String geohash = "";
        String visibility = "";
        float snowcover = 0;
        while (itr.hasMoreTokens()) {
            String featrue = itr.nextToken();
            if (i == 0) {
                timestamp = featrue;
            }
            if (i == 1) {
                geohash = featrue;
            }
            if (i == 7) {
                visibility = featrue;
            }
            if (i == 41) {
                snowcover = Float.valueOf(featrue);
            }
            i++;
        }
        if (geohash.matches("(9xh3).*") && (snowcover==100)) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis( Long.valueOf( timestamp ) );
            String day = calendar.get(Calendar.YEAR) + String.format("%02d", calendar.get(Calendar.MONTH)+1) + String.format("%02d",calendar.get(Calendar.DATE ));
            context.write(new Text("Vail Ski Resort"), new Text( day + ":" + visibility));
        }

    }
}
