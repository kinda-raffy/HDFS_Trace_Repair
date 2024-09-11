package org.apache.hadoop.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MetricTimer {
    static final String metric_path = "metrics.txt";
    long thread;
    BufferedWriter writer;

    public MetricTimer(long thread) {
        this.thread = thread;
        try {
            writer = Files.newBufferedWriter(Paths.get(metric_path), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start(String label) {
        long timestamp = System.currentTimeMillis();
        try {
            writer.write(timestamp + "\t" + thread + "\tSTART\t" + label);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void end(String label) {
        long timestamp = System.currentTimeMillis();
        try {
            writer.write(timestamp + "\t" + thread + "\tEND\t" + label);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
