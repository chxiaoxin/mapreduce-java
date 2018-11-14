package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class writableSerilize {

    public static void main(String[] args) throws IOException {
        IntWritable intw = new IntWritable(19);
        System.out.println(serilize(intw).length);
        IntWritable intwOut = new IntWritable();
        deserilize(intwOut, serilize(intw));
        System.out.println(intwOut.get());
    }

    public static void deserilize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(in);
        writable.readFields(din);
    }
    public static byte[] serilize(Writable writable) throws IOException {
        ByteArrayOutputStream byteout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteout);
        writable.write(out);
        out.close();
        return byteout.toByteArray();
    }
}
