package com.lean;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @author Sabo
 *
 */
public class FileSystemDoubleCat {
    
    public static void main(String[] args) throws Exception{
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;
        
        try {
            in = fs.open(new Path(uri));
            //fist print
            IOUtils.copyBytes(in, System.out, 4098, false);
            //go back to the start of file
            in.seek(0);
            //second print
            IOUtils.copyBytes(in, System.out, 4098, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

}
