package edu.usfca.cs.mr.mystartup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/**
 * Created by xuekang on 11/5/17.
 */
public class AmarilloJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn
            // webapp.
            Job job = Job.getInstance(conf, "Amarillo Job");
            // Current class.
            job.setJarByClass(AmarilloJob.class);
            // Mapper
            job.setMapperClass(AmarilloMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            //job.setCombinerClass(AmarilloReducer.class);
            // Reducer
            job.setReducerClass(AmarilloReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties if the Mapper and Reducer has same key and value
            // types. It is set separately for elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
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

//public class AmarilloJob {
//    public static void main(String[] args) {
//        String inputPath = "input/nam_mini.tdv";
//        String outputPath = "output/surfingOutPut";
//
//        try {
//            Configuration conf = new Configuration();
//            // Give the MapRed job a name. You'll see this name in the Yarn
//            // webapp.
//            Job job = Job.getInstance(conf, "Hottest temperature count job");
//            // Current class.
//            job.setJarByClass( AmarilloJob.class);
//            // Mapper
//            job.setMapperClass(AmarilloMapper.class);
//            // Combiner. We use the reducer as the combiner in this case.
//            job.setCombinerClass(AmarilloReducer.class);
//            // Reducer
//            job.setReducerClass(AmarilloReducer.class);
//            // Outputs from the Mapper.
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(Text.class);
//            // Outputs from Reducer. It is sufficient to set only the following
//            // two properties if the Mapper and Reducer has same key and value
//            // types. It is set separately for elaboration.
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(Text.class);
//            // path to input in HDFS
////            FileInputFormat.addInputPath(job, new Path(inputPath));
//            FileInputFormat.addInputPath(job, new Path(args[0]));
//            // path to output in HDFS
////            FileOutputFormat.setOutputPath(job, new Path(outputPath));
//            FileOutputFormat.setOutputPath(job, new Path(args[1]));
//            // Block until the job is completed.
//            System.exit(job.waitForCompletion(true) ? 0 : 1);
//        } catch (IOException e) {
//            System.err.println(e.getMessage());
//        } catch (InterruptedException e) {
//            System.err.println(e.getMessage());
//        } catch (ClassNotFoundException e) {
//            System.err.println(e.getMessage());
//        }
//    }
//}
