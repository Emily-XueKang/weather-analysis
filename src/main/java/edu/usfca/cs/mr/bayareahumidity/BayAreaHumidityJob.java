package edu.usfca.cs.mr.bayareahumidity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

/**
 * Created by xuekang on 11/2/17.
 */
public class BayAreaHumidityJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn
            Job job = Job.getInstance(conf, "bay area humidity job");
            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            // Current class.
            job.setJarByClass(BayAreaHumidityJob.class);
            // Mapper
            job.setMapperClass(BayAreaHumidityMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(BayAreaHumidityReducer.class);
            // Reducer
            job.setReducerClass(BayAreaHumidityReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(FloatWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties if the Mapper and Reducer has same key and value
            // types. It is set separately for elaboration.
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(FloatWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
