package edu.usfca.cs.mr.mystartup;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringTokenizer;
/**
 * Created by xuekang on 11/5/17.
 */
public class YelloStoneNationalParkReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float bestVis = 0;
        String bestTime = "";
        for(Text val : values) {
            String value = val.toString();
            String[] features = value.split( "\t" );
            String day = features[0];
            Float visibility = Float.valueOf(features[1]);
            if (bestVis <= Float.valueOf(visibility)) {
                bestVis = Float.valueOf(visibility);
                bestTime = day;
            }
        }
        context.write(new Text(bestTime), new Text(String.valueOf(bestVis)));
    }
}
