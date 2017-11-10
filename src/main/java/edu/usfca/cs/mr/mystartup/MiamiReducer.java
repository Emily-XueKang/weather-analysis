package edu.usfca.cs.mr.mystartup;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by xuekang on 11/7/17.
 */
public class MiamiReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String precipitation = "";
        boolean lessthan25 = false;
        Float prec;
        for(Text val : values){
            precipitation = val.toString();
            prec = Float.valueOf(precipitation);
            if(prec<50){
                lessthan25=true;
            }
            else{
                lessthan25=false;
            }
        }
        if(lessthan25){
            context.write(new Text(key),new Text(precipitation));
        }
    }

}

