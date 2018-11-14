package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class streamCompressor {

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        String compressor = "org.apache.hadoop.io.compress.GzipCodec";
        Class<?> className = Class.forName(compressor);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(className, conf);
        CompressionOutputStream out = codec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in, out, 4096, false);
    }
}
