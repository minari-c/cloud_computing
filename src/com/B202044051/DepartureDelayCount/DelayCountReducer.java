package com.B202044051.DepartureDelayCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class DelayCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    private MultipleOutputs<Text, IntWritable> mos;

    // setup
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text, IntWritable>(context);
    }


    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values) {
            sum += value.get();     // 횟수 더하기
        }

        result.set(sum);
        // context.write(key, result);
        // mos.write
        mos.write("departure", key, result);
    }

    // close
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
