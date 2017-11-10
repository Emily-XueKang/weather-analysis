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
        float visThreshHold = 22000;
        boolean goodVis = false;
        float visibility = 0;
        for(Text val : values) {
            visibility = Float.valueOf(val.toString());
            if(visibility>visThreshHold){
                goodVis = true;
            }
            else{
                goodVis = false;
            }
        }
        if(goodVis==true){
            context.write(key, new Text(String.valueOf(visibility)));
        }
    }
}
