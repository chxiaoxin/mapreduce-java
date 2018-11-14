package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

public class joins {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path inputl = new Path("/Users/lixin/Desktop/dept.txt");
        Path inputr = new Path("/Users/lixin/Desktop/employee.txt");
        Path output = new Path("/Users/lixin/Desktop/leftouter");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(joins.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(reducer.class);

        MultipleInputs.addInputPath(job, inputl, TextInputFormat.class, mapperLeft.class);
        MultipleInputs.addInputPath(job, inputr, TextInputFormat.class, mapperRight.class);

        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true)? 0:1);

    }

    public static class mapperLeft extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split(",");
            String id = row[0];
            String dept = row[1];
            String location = row[2];
            context.getCounter("mapperl","lcounter").increment(1);
            context.write(new Text(id), new Text("l," + dept + "," + location));
        }
    }

    public static class mapperRight extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] row = value.toString().split(",");
            String depId = row[row.length - 1];
            String[] others = Arrays.copyOfRange(row, 0, row.length - 1);
            String other = Arrays.stream(others).reduce( "r", (a, b) -> a + "," + b);
            context.getCounter("mapperr", "rcounter").increment(1);
            context.write(new Text(depId), new Text(other));

        }
    }

    public static class reducer extends Reducer<Text, Text, Text, Text> {

        public static void addE (ArrayList<String> array, String[] rowArray) {
            String[] remains = Arrays.copyOfRange(rowArray, 1, rowArray.length);
            Optional<String> streamString = Arrays.stream(remains).reduce((a, b) -> a + ","+ b);
            array.add(streamString.get());
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> larray = new ArrayList<String>();
            ArrayList<String> rarray = new ArrayList<String>();
            for (Text value: values) {
                String row = value.toString();
                String[] rowArray = row.split(",");
                String prefix = row.split(",")[0];
                if (prefix.equals("l")) {
                    addE(larray, rowArray);
                } else {
                    addE(rarray, rowArray);
                }
            }
            if (!larray.isEmpty()) {
                for (String lrow: larray) {
                    if (!rarray.isEmpty()) {
                        for (String rrow: rarray) {
                            context.write(key, new Text(lrow + "," + rrow));
                        }
                    } else {
                        context.write(key, new Text(lrow + "," + "null,null,null,null,null"));
                    }
                }
            }
        }
    }
}
