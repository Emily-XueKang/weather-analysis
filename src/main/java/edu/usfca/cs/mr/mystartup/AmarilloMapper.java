package edu.usfca.cs.mr.mystartup;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.StringTokenizer;
/**
 * Created by xuekang on 11/5/17.
 */
//http://shawnvoyage.com/top-5-windiest-cities-in-the-world/
//30.categorical_rain_yes1_no0_surface
//16.surface_wind_gust_surface
//Wellington geohahs: 9wr8|9wrb|9wpx|9wpz
public class AmarilloMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        ArrayList<String> oneRecord = new ArrayList<>();
        while(itr.hasMoreTokens()){
            oneRecord.add(itr.nextToken());
        }
        String timestamp = oneRecord.get(0);
        String geohash = oneRecord.get(1);
        String rainOrNot = oneRecord.get(29);
        String windGust = oneRecord.get(15);

        if ((rainOrNot.equals("0.0")) && (geohash.matches("(9wr8|9wrb|9wpx|9wpz).*"))) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(Long.valueOf(timestamp));
            String day = calendar.get(Calendar.YEAR) + String.format("%02d", calendar.get(Calendar.MONTH)+1) + calendar.get(Calendar.DATE ) ;
            context.write(new Text("Amarillo Best Time"), new Text(day + "\t" + windGust));
        }
    }
}
