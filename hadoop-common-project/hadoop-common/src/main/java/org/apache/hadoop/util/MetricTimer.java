package org.apache.hadoop.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class MetricTimer {
    static final String metric_path = "metrics.txt";
    long thread;

    public MetricTimer(long thread) {
        this.thread = thread;
    }

    public void mark(String label) {
        long timestamp = System.currentTimeMillis();
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(metric_path, true))) {
                writer.write(thread + "\t" + label + "\t" + timestamp);
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        future.exceptionally(ex -> {
            ex.printStackTrace();
            return null;
        });
    }
}
