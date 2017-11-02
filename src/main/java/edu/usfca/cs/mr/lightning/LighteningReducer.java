package edu.usfca.cs.mr.lightning;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xuekang on 10/31/17.
 */
public class LighteningReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    Map<String, Float> geoMap = new HashMap<String, Float>();
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float count = 0;
        // calculate the total count of lightenings per geohash
        for (FloatWritable val : values) {
            count += val.get();
        }
        geoMap.put(key.toString(), count);
        System.out.println("====put geomap key==="+key);
        System.out.println("====put lightening value ==="+count);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("====unsorted Map size ==="+geoMap.size());
        Map<String, Float> sortedMap = sortByValue(geoMap);
        int counter = 0;
        System.out.println("====sorted Map size==="+sortedMap.size());
        for (String key: sortedMap.keySet()) {
            if(counter++ == 3) break;
            context.write(new Text(key), new FloatWritable(sortedMap.get(key)));
            System.out.println("====writh key==="+key);
            System.out.println("====wirte value ==="+sortedMap.get(key));
        }
    }

    private static Map<String, Float> sortByValue(Map<String, Float> unsortMap) {
        // 1. Convert Map to List of Map
        List<Map.Entry<String, Float>> list =
                new LinkedList<Map.Entry<String, Float>>(unsortMap.entrySet());
        // 2. Sort list with Collections.sort(), provide a custom Comparator
        //    Try switch the o1 o2 position for a different order
        Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
            @Override
            public int compare(Map.Entry<String, Float> o1,
                               Map.Entry<String, Float> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });
        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
        Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
        for (Map.Entry<String, Float> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
            System.out.println("====list entry key==="+entry.getKey());
            System.out.println("====list entry value==="+entry.getValue());
        }
        return sortedMap;
    }
}


