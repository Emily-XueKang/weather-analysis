package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by xuekang on 10/31/17.
 */
public class LighteningReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    Map<Text, IntWritable> geoMap = new HashMap<Text, IntWritable>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for (IntWritable val : values) {
            count += val.get();
        }
        geoMap.put(key, new IntWritable(count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, IntWritable> sortedMap = sortByValue(geoMap);
        int counter = 0;
        for (Text key: sortedMap.keySet()) {
            if (counter++ == 20) break;
            context.write(key, sortedMap.get(key));
        }
    }

    private static Map<Text, IntWritable> sortByValue(Map<Text, IntWritable> unsortMap) {

        // 1. Convert Map to List of Map
        List<Map.Entry<Text, IntWritable>> list =
                new LinkedList<Map.Entry<Text, IntWritable>>(unsortMap.entrySet());

        // 2. Sort list with Collections.sort(), provide a custom Comparator
        //    Try switch the o1 o2 position for a different order
        Collections.sort(list, new Comparator<Map.Entry<Text, IntWritable>>() {
            public int compare(Map.Entry<Text, IntWritable> o1,
                               Map.Entry<Text, IntWritable> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
        Map<Text, IntWritable> sortedMap = new LinkedHashMap<Text, IntWritable>();
        for (Map.Entry<Text, IntWritable> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }
}


