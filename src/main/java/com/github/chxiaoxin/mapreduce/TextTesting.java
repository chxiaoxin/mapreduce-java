package com.github.chxiaoxin.mapreduce;

import org.apache.hadoop.io.Text;

public class TextTesting {
    public static void main(String[] args) {
        Text text = new Text("\u0041\u00DF\u6771\uD801\uDC00");
        System.out.println(text.find("\u0041"));
        System.out.println(text.find("\u6771"));
        System.out.println(text.getLength());
        System.out.println(text.getBytes());
    }
}
