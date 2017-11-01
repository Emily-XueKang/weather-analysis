package edu.usfca.cs.mr.lightning;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by xuekang on 11/1/17.
 */
public class LighteningJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn
            // webapp.
            Job job = Job.getInstance(conf, "lightening place job");
            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            // Current class.
            job.setJarByClass(LighteningJob.class);
            // Mapper
            job.setMapperClass(LighteningMapper.class);
            // Reducer
            job.setReducerClass(LighteningReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties if the Mapper and Reducer has same key and value
            // types. It is set separately for elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            //TODO:find the highest lightening count among all output files
//            File[] files = new File(args[1]).listFiles();
//            for (File file : files) {
//                if (file.isDirectory()) {
//                    System.out.println("Directory: " + file.getName());
//                } else {
//                    System.out.println("File: " + file.getName());
//                    //read and compare
//
//                }
//            }
            // Block until the job is completed.
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
