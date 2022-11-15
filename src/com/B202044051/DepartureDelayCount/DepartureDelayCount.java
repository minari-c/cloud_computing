package com.B202044051.DepartureDelayCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// drive file
public class DepartureDelayCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if(args.length != 3) {
            System.err.println("Usage: DepartureDelayCount <input> <output>");
            System.exit(2);
        }

        Job job = new Job(conf, "DepartureDelayCount");

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setJarByClass(DepartureDelayCount.class);
        job.setMapperClass(DepartureDelayCountMapper.class);
        job.setReducerClass(DelayCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        MultipleOutputs.addNamedOutput(
                job
                , "departure"
                , TextOutputFormat.class
                , Text.class
                , IntWritable.class
        );

        job.waitForCompletion(true);
    }
}
