package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class distributedCache {

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Path input = new Path("/Users/lixin/Desktop/emp.txt");
        Path output = new Path("/Users/lixin/Desktop/empIncrement");
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.addCacheFile(new URI("/Users/lixin/Desktop/pairs.txt"));
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setJarByClass(distributedCache.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private HashMap<String, Integer> map = new HashMap<String, Integer>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] uris = context.getCacheFiles();
            Logger logger = LoggerFactory.getLogger(mapper.class);
            for (URI uri : uris) {
                logger.info(uri.toString());
                try {
                    if (uri.compareTo(new URI("/Users/lixin/Desktop/pairs.txt")) == 0) {
                        BufferedReader reader = new BufferedReader(new FileReader(uri.toString()));
                        String line = reader.readLine();
                        while (line != null) {
                            String[] elements = line.split(",");
                            String key = elements[0];
                            int value = Integer.parseInt(elements[1]);
                            map.put(key, value);
                            logger.info(line);
                            line = reader.readLine();
                        }
                    }
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            String empNo = values[0];
            String title = values[2];
            float salary = Float.parseFloat(values[3]);
            int increment = map.getOrDefault(title.toLowerCase(), 0);
            float increase = salary * increment / 100;
            context.write(new Text(empNo), new FloatWritable(increase));
        }
    }

    public static class reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            for (FloatWritable value: values) {
                context.write(key, value);
            }
        }
    }
}
