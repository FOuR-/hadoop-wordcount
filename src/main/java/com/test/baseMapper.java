package com.test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class baseMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        StringTokenizer tokenizerValue = new StringTokenizer(value.toString());

        while (tokenizerValue.hasMoreElements()) {
            word.set(tokenizerValue.nextToken());
            context.write(word, one);
        }

    }

}
