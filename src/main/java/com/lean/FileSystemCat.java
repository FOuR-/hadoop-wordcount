package com.lean;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;



/**
 * run
 * mvn clean package
 * hadoop jar /Users/Sabo/tiny/Documents/workspace/wordcount/target/wordcount-0.0.1-SNAPSHOT.jar com.lean.FileSystemCat /user/hadoop/wordcount/dual.sql
 */
public class FileSystemCat {
    
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        
        InputStream in = null ;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
        
    }

}
