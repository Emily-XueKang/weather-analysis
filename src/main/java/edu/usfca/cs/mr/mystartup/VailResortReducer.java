package edu.usfca.cs.mr.mystartup;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
/**
 * Created by xuekang on 11/7/17.
 */
public class VailResortReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float maxVisibility = 0;
        String visibility = "";
        String day = "";
        String besttime = "";
        for(Text val : values){
            String value = val.toString();
            String[] split = value.split(":");
            day = split[0];
            visibility = split[1];
            if (maxVisibility < Float.valueOf(visibility)) {
                maxVisibility = Float.valueOf(visibility);
                besttime = day;
            }
        }
        context.write(key, new Text(besttime+ " : " + maxVisibility));
    }
}
