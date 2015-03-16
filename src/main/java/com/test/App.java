package com.test;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * run: hadoop jar *.jar com.test.App
 */
public class App extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new App(), args));
    }

    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Path input = new Path("/user/hadoop/wordcount");
        Path output = new Path("/user/hadoop/output");
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(output, true);
        
        Job job = new Job(getConf());
        job.setJarByClass(App.class);
        
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        
        job.setMapperClass(baseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setReducerClass(baseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        if (job.waitForCompletion(true)) {

            InputStream in = fs.open(new Path(output, "part-r-00000"));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            return 0;
        } else {
            return 2;
        }
        
    }

}
