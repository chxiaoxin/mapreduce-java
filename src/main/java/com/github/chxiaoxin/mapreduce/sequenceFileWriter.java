package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

public class sequenceFileWriter {
    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws IOException {
        String uri = "/Users/lixin/Desktop/abc";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        Path path = new Path(uri);
        SequenceFile.Writer writer = null;
        EnumSet enumSet = EnumSet.of(CreateFlag.CREATE);
        writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
        for (int i = 0; i<100; i++){
            key.set(i);
            value.set(DATA[i % DATA.length]);
            System.out.println(key.toString() + "\t" + value);
            writer.append(key, value);
        }
        IOUtils.closeStream(writer);
    }
}
