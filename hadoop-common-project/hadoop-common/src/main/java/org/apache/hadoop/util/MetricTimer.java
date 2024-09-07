package org.apache.hadoop.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class MetricTimer {
    static final String metric_path = "metrics.txt";
    long thread;

    public MetricTimer(long thread) {
        this.thread = thread;
    }

    public void start(String label) {
        long timestamp = System.currentTimeMillis();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(metric_path, true))) {
            writer.write(thread + "\tSTART\t" + label + "\t" + timestamp);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void end(String label) {
        long timestamp = System.currentTimeMillis();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(metric_path, true))) {
            writer.write(thread + "\tEND\t" + label + "\t" + timestamp);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
