package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.concurrent.TimeUnit;


public class wordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("wordCount-hello world");
        Path input = new Path("/Users/lixin/Desktop/fraud.txt");
        Path output = new Path("/Users/lixin/Desktop/fraudpts");
        job.setJarByClass(wordCount.class);
        job.setMapperClass(mapperForWordCount.class);
        job.setReducerClass(reducerForWordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(fraudWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class mapperForWordCount extends Mapper<LongWritable, Text, Text, fraudWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String record = value.toString();
           String[] row = record.split(",");
           String userid = row[0];
           String receivingDate = row[5];
           Boolean isReturnOrder = row[6].equals("yes") ? true : false;
           String returnDate = row[7];
           Logger logger = LoggerFactory.getLogger(mapperForWordCount.class);
           logger.info(Boolean.toString(isReturnOrder) + "<=>" + row[6]);
           fraudWritable fraudwritable = new fraudWritable(returnDate, receivingDate, isReturnOrder);
           context.write(new Text(userid), fraudwritable);
        }
    }

    public static class reducerForWordCount extends Reducer<Text, fraudWritable, Text, IntWritable> {

        private ArrayList<String> list = new ArrayList<String>();

        public void reduce(Text key, Iterable<fraudWritable> valueIterable, Context context) throws IOException, InterruptedException {
            int totalOrder = 0;
            int returnOrder = 0;
            int fraudPoints = 0;
            SimpleDateFormat simpleformat = new SimpleDateFormat("dd-MM-yyyy");
            Logger logger = LoggerFactory.getLogger(reducerForWordCount.class);
            for (fraudWritable value : valueIterable) {
                totalOrder += 1;
                String returnDate = value.getReturnDate();
                String receiveDate = value.getReceivingDate();
                Boolean isReturn = value.getIsReturnOrder();
                if (isReturn) {
                    returnOrder += 1;
                }
                try {

                    Date returnDt = simpleformat.parse(returnDate);
                    Date receiveDt = simpleformat.parse(receiveDate);
                    Long interval = TimeUnit.DAYS.convert(returnDt.getTime() - receiveDt.getTime(), TimeUnit.MILLISECONDS);
                    if (interval > 10) {
                        fraudPoints += 1;
                    }
                } catch (ParseException e) {
                    logger.error("cannot parse");
                }
            }
            float rate = (new Float(returnOrder) / totalOrder);
            if (rate > 0.5) {
                fraudPoints += 10;
            }
            list.add(key + "_" + Integer.toString(fraudPoints));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(list, new Comparator<String>() {
                public int compare(String o1, String o2) {
                    int r1 = Integer.parseInt(o1.split("_")[1]);
                    int r2 = Integer.parseInt(o2.split("_")[1]);
                    return r2 - r1;
                }
            });
            for (String e: list) {
                String key = e.split("_")[0];
                int pts = Integer.parseInt(e.split("_")[1]);
                context.write(new Text(key), new IntWritable(pts));
            }
        }
    }
}
