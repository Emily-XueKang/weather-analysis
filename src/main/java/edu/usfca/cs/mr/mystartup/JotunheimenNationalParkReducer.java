package edu.usfca.cs.mr.mystartup;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringTokenizer;
/**
 * Created by xuekang on 11/5/17.
 */
public class JotunheimenNationalParkReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float bestVis = 0;
        int i = 0;
        String bestTime = "";
        for(Text val : values) {
            StringTokenizer itr = new StringTokenizer(val.toString());
            String yearmonth = "";
            String visibility = "";
            while (itr.hasMoreTokens()) {
                String item = itr.nextToken();
                if (i == 0) {
                    yearmonth = item;
                }
                if (i == 1) {
                    visibility = item;
                }
                i++;
            }

            if (bestVis >= Float.valueOf(visibility)) {
                bestVis = Float.valueOf(visibility);
                bestTime = yearmonth;
            }
        }
        context.write(key, new Text(String.valueOf(bestTime)));
    }
}
