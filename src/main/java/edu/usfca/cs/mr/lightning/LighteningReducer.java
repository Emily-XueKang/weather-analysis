package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by xuekang on 10/31/17.
 */
public class LighteningReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    Map<Text, FloatWritable> geoMap = new HashMap<Text, FloatWritable>();

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for (FloatWritable val : values) {
            count += val.get();
        }
        geoMap.put(key, new FloatWritable(count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, FloatWritable> sortedMap = sortByValue(geoMap);
        int counter = 0;
        for (Text key: sortedMap.keySet()) {
            if (counter++ == 3) break;
            context.write(key, sortedMap.get(key));
        }
    }

    private static Map<Text, FloatWritable> sortByValue(Map<Text, FloatWritable> unsortMap) {

        // 1. Convert Map to List of Map
        List<Map.Entry<Text, FloatWritable>> list =
                new LinkedList<Map.Entry<Text, FloatWritable>>(unsortMap.entrySet());

        // 2. Sort list with Collections.sort(), provide a custom Comparator
        //    Try switch the o1 o2 position for a different order
        Collections.sort(list, new Comparator<Map.Entry<Text, FloatWritable>>() {
            public int compare(Map.Entry<Text, FloatWritable> o1,
                               Map.Entry<Text, FloatWritable> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
        Map<Text, FloatWritable> sortedMap = new LinkedHashMap<Text, FloatWritable>();
        for (Map.Entry<Text, FloatWritable> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }
}


