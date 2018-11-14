package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.IOException;

public class multipleOutput {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inputPath = new Path("/Users/lixin/Desktop/mo.txt");
        Path outPath = new Path("/Users/lixin/Desktop/multiple_outputs");

        Configuration conf  = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(mutilpelnputs.class);
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);


        MultipleOutputs.addNamedOutput(job, "HR", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "Accounts", TextOutputFormat.class, Text.class, IntWritable.class);

        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true)? 0:1);
    }

    public static class mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(",");
            String name = words[1];
            String depart = words[2];
            String salary = words[3];
            context.write(new Text(name + "_" + depart), new Text(salary));
        }
    }

    public static class reducer extends Reducer<Text, Text, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> out;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            out = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int salaryTotal = 0;
            for(Text value: values) {
                String salary = value.toString();
                salaryTotal += Integer.parseInt(salary);
            }
            String depart = key.toString().split("_")[1];
            String name = key.toString().split("_")[0];
            Logger logger = LoggerFactory.getLogger(reducer.class);
            logger.info(depart);
            logger.info(Boolean.toString(depart.equals("HR")));
            if (depart.equals("HR")) {
                out.write("HR", new Text(name), new IntWritable(salaryTotal));
            } else {
                out.write("Accounts", new Text(name), new IntWritable(salaryTotal));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            out.close();
        }
    }

}
