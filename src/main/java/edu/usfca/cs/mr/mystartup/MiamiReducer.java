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
        boolean lessthan30 = false;
        Float prec;
        for(Text val : values){
            precipitation = val.toString();
            prec = Float.valueOf(precipitation);
            if(prec<30){
                lessthan30=true;
            }
            else{
                lessthan30=false;
            }
        }
        if(lessthan30){
            context.write(new Text(key),new Text(precipitation));
        }
    }

}

