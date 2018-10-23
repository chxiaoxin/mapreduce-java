package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class assignment1 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("/Users/lixin/Desktop/fb2.txt");
        Path output = new Path("/Users/lixin/Desktop/fb_suc_rate");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(assignment1.class);
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true)? 0:1);
    }

    public static class mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] values = valueString.split(",");
            String date = values[1];
            String city = values[2];
            String category = values[3];
            String total = values[4];
            String success = values[5];
            Float rate = Float.parseFloat(success)/Float.parseFloat(total);
            String outkey = category + " " + city + " " + date.split("-")[1];
            context.write(new Text(outkey), new FloatWritable(rate));
        }
    }

    public static class reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private ArrayList<String> arrays = new ArrayList<String>();
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0f;
            int count = 0;
            for (FloatWritable value : values) {
                sum += value.get();
                count += 1;
            }
            String rate = Float.toString(sum/count);
            arrays.add(key + "_" + rate);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(arrays);
            for (String e : arrays) {
                String key = e.split("_")[0];
                Float value = Float.parseFloat(e.split("_")[1]);
                context.write(new Text(key), new FloatWritable(value));
            }
        }
    }
}
