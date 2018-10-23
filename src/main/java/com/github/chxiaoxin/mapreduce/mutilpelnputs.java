package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.awt.*;
import java.io.IOException;

public class mutilpelnputs {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path word1Path = new Path("/Users/lixin/Desktop/word1.txt");
        Path word2Path = new Path("/Users/lixin/Desktop/word2.txt");
        Path outPath = new Path("/Users/lixin/Desktop/multiple_inputs");

        Configuration conf  = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(mutilpelnputs.class);
        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job, word1Path, TextInputFormat.class, mapper1.class);
        MultipleInputs.addInputPath(job, word2Path, TextInputFormat.class, mapper2.class);

        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true)? 0:1);
    }

    public static class mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word: words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    public static class mapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String word = value.toString().split(",")[1];
            context.write(new Text(word), new IntWritable(1));
        }
    }

    public static class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
