package com.tutorial.testapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestNewAPI extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TestNewAPI(), args));
    }
    
    public int run(String[] args) throws Exception {
        
        if (args.length < 2) {
            System.err.println("Usage: hadoop jar wordcount.jar <inputpath> <outpath> [overwrite]");
            System.exit(1);
        }
        
        Job job = new Job(getConf());
        job.setJarByClass(TestNewAPI.class);
        job.setJobName("Test new api");
        
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        
        if (args.length ==  3 && "true".equalsIgnoreCase(args[2])) {
            FileSystem fs = FileSystem.get(getConf());
            fs.deleteOnExit(output);
        }
        
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        
        job.setMapperClass(baseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setReducerClass(baseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    static class baseMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        
        private final LongWritable ONE  = new LongWritable(1);
        
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            
            String[] words = value.toString().split("\\s+");
            for (String word : words) {
                context.write(new Text(word), ONE);
            }
        }
        
    }
    
    static class baseReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            
            Long count = new Long(0);
            for (LongWritable value : values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
        
    }

}
