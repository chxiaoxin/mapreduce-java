package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

public class sequenceFileReader {

    public static void main(String[] args) throws IOException {
        String uri = "/Users/lixin/Desktop/abc";
        Path path = new Path(uri);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        long pos = reader.getPosition();
        Writable key = ReflectionUtils.newInstance(IntWritable.class, conf);
        Writable value = ReflectionUtils.newInstance(Text.class, conf);
        while(reader.next(key, value)) {
            System.out.println(key.toString() + "\t" + value.toString());
        }
    }
}
