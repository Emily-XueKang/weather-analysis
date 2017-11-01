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

    Map<String, Float> geoMap = new ConcurrentHashMap<String, Float>();
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float count = 0;
        // calculate the total count
        for (FloatWritable val : values) {
            count += val.get();
        }
        geoMap.put(key.toString(), count);
        System.out.println("====put map key==="+key);
        //context.write(key, new FloatWritable(count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<String, Float> sortedMap = sortByValue(geoMap);
        int counter = 0;
        System.out.println("====map size==="+sortedMap.size());
        for (String key: sortedMap.keySet()) {
            if(counter++ == 3) break;
            context.write(new Text(key), new FloatWritable(sortedMap.get(key)));
        }
    }


    private static Map<String, Float> sortByValue(Map<String, Float> unsortMap) {
        // 1. Convert Map to List of Map
        List<Map.Entry<String, Float>> list =
                new LinkedList<Map.Entry<String, Float>>(unsortMap.entrySet());

        /*
        List<Map.Entry<Text, FloatWritable>> list =
                new LinkedList<Map.Entry<Text, FloatWritable>>();
        for (Map.Entry<Text, FloatWritable> entry: unsortMap.entrySet()) {
            list.add(entry);
            System.out.println("====list entrykey==="+entry.getKey());
        }
        */
        // 2. Sort list with Collections.sort(), provide a custom Comparator
        //    Try switch the o1 o2 position for a different order
        Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
            @Override
            public int compare(Map.Entry<String, Float> o1,
                               Map.Entry<String, Float> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
        Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
        for (Map.Entry<String, Float> entry : list) {
            System.out.println("====list entrykey2==="+entry.getKey());
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        System.out.println("====sorted map size==="+sortedMap.size());
        return sortedMap;
    }
    //    private static Map<Text, FloatWritable> sortByValue(Map<Text, FloatWritable> unsortMap) {
//        // 1. Convert Map to List of Map
//        List<Map.Entry<Text, FloatWritable>> list =
//                new LinkedList<Map.Entry<Text, FloatWritable>>(unsortMap.entrySet());
//
//        /*
//        List<Map.Entry<Text, FloatWritable>> list =
//                new LinkedList<Map.Entry<Text, FloatWritable>>();
//        for (Map.Entry<Text, FloatWritable> entry: unsortMap.entrySet()) {
//            list.add(entry);
//            System.out.println("====list entrykey==="+entry.getKey());
//        }
//        */
//        // 2. Sort list with Collections.sort(), provide a custom Comparator
//        //    Try switch the o1 o2 position for a different order
//        Collections.sort(list, new Comparator<Map.Entry<Text, FloatWritable>>() {
//            @Override
//            public int compare(Map.Entry<Text, FloatWritable> o1,
//                               Map.Entry<Text, FloatWritable> o2) {
//                return (o1.getValue()).compareTo(o2.getValue());
//            }
//        });
//
//        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
//        Map<Text, FloatWritable> sortedMap = new LinkedHashMap<Text, FloatWritable>();
//        for (Map.Entry<Text, FloatWritable> entry : list) {
//            System.out.println("====list entrykey2==="+entry.getKey());
//            sortedMap.put(entry.getKey(), entry.getValue());
//        }
//        System.out.println("====sorted map size==="+sortedMap.size());
//        return sortedMap;
//    }
}


