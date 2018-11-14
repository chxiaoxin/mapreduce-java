package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class hdfsJavaInterface {

    private static void readFile(String uri) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), conf);
        InputStream stream = fs.open(new Path(uri));
        IOUtils.copyBytes(stream, System.out, 4096);
        IOUtils.closeStream(stream);
    }

    private static void copyFile(String dest, String localFile) throws IOException, URISyntaxException {
        InputStream input = new BufferedInputStream(new FileInputStream(localFile));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(dest), conf);
        OutputStream out = fs.create(new Path(dest), new Progressable() {
            @Override
            public void progress() {
                System.out.print("-");
            }
        });
        IOUtils.copyBytes(input, out, 64);
        IOUtils.closeStream(input);
    }

    private static void getStatus(String uri) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), conf);
        FileStatus status = fs.getFileStatus(new Path(uri));
        System.out.println(status.getBlockSize());
        System.out.println(status.getLen());
        System.out.println(status.getPermission().toString());


    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        getStatus("/Users/lixin/Desktop/copyfile");
    }
}
