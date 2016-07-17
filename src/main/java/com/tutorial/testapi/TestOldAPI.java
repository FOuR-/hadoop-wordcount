package com.tutorial.testapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TestOldAPI {
    
    public static void main(String[] args) throws IOException {
        
        if (args.length < 2) {
            System.err.println("Usage: hadoop jar wordcount.jar <inputpath> <outpath> [overwrite]");
            System.exit(1);
        }
        
        JobConf job = new JobConf();
        job.setJarByClass(TestOldAPI.class);
        
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        
        if (args.length ==  3 && "true".equalsIgnoreCase(args[2])) {
            FileSystem fs = FileSystem.get(job);
            fs.deleteOnExit(output);
        }
        
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        
        job.setMapperClass(baseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setReducerClass(baseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        JobClient.runJob(job);
    }
    
    static class baseMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

        private static final LongWritable ONE = new LongWritable(1);
        
        public void map(LongWritable arg0, Text arg1, OutputCollector<Text, LongWritable> arg2, Reporter arg3)
                throws IOException {
            
            String words[] = arg1.toString().split("\\s+");
            
            for (String word : words) {
                arg2.collect(new Text(word), ONE);
            }
            
        }
        
    }
    
    static class baseReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text arg0, Iterator<LongWritable> arg1, OutputCollector<Text, LongWritable> arg2,
                Reporter arg3) throws IOException {
            
            Long count = new Long(0);
            
            while(arg1.hasNext()) {
                count += arg1.next().get();
            }
            
            arg2.collect(arg0, new LongWritable(count));
        }
        
    }
    
}
