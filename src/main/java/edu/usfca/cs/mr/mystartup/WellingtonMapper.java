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
//29. categorical_rain_yes1_no0_surface
//37. v-component_of_wind_maximum_wind
//53. u-component_of_wind_maximum_wind
//Wellington geohahs: rbsj,rbsm,rbsh,rbsk
public class WellingtonMapper extends Mapper<LongWritable, Text, Text, Text>{
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
        String v_wind = oneRecord.get(37);
        String u_wind = oneRecord.get(53);

        if ((rainOrNot.equals("0.0")) && (geohash.matches("(rbsj|rbsm|rbsh|rbsk).*"))) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(Long.valueOf(timestamp));
            String yearmonth = calendar.get(Calendar.YEAR) + String.format("%02d", calendar.get(Calendar.MONTH)+1);
            context.write(new Text("Wellington Best Time"), new Text(yearmonth + "\t" + v_wind + "\t" + u_wind));
        }
    }
}
