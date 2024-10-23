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

    public void start(String label, String... metadata) {
        long timestamp = System.currentTimeMillis();
        String line = timestamp + "\t" + thread + "\tSTART\t" + label;

        if (metadata.length > 0) {
            line += "\t";
            for (String m : metadata) {
                line += m;
            }
        }

        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void end(String label, String... metadata) {
        long timestamp = System.currentTimeMillis();
        String line = timestamp + "\t" + thread + "\tEND\t" + label;

        if (metadata.length > 0) {
            line += "\t";
            for (String m : metadata) {
                line += m;
            }
        }
        
        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
