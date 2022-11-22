package com.B202044051.SecondarySort;

import com.B202044051.SecondarySort.AirlinePerformanceParser;
import com.B202044051.SecondarySort.DelayCounters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class DepartureDelayCountMapperWithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable> {
    private final static IntWritable outputValue = new IntWritable(1);
    private DateKey outputKey = new DateKey();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

        // 이 부분에서 에러 발생했음.
        // Departure랑 Arrival 모두 들어올 수 있으니 각각 if로 조건 부여해야한다.
        if (parser.isDepartureDelayAvailable()) {
            if (parser.getDepartureDelayTime() > 0) {
                outputKey.setYear(String.format("D,%s", parser.getYear()));
                outputKey.setMonth(parser.getMonth());
                // outputKey.set(String.format("D,%s,%s", parser.getYear(), parser.getMonth()));
                context.write(outputKey, outputValue);
            }
            else if (parser.getDepartureDelayTime() == 0)
            {
                context.getCounter(DelayCounters.scheduled_departure).increment(1);
            }
        }

        if (parser.isArrivalDelayAvailable()) {
            if (parser.getArrivalDelayTime() > 0) {
                outputKey.set(String.format("A,%s,%s", parser.getYear(), parser.getMonth()));
                context.write(outputKey, outputValue);
            }
        }
    }
}
