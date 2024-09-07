package org.apache.hadoop.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MetricTimer {
    static final String metric_path = "metrics.txt";

    long threadId;

    private final BlockingQueue<String> queue;
    private final Thread thread;

    public MetricTimer(long threadId) {
        this.threadId = threadId;
        queue = new LinkedBlockingQueue<>();
        thread = new Thread(this::processQueue);
        thread.start();
    }

    private void processQueue() {
        while (true) {
            try {
                String content = queue.take();
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(metric_path,
                        true))) {
                    writer.write(content);
                    writer.newLine();
                } catch (IOException x) {
                    System.err.format("IOException: %s%n", x);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void start(String label) {
        long timestamp = System.currentTimeMillis();
        try {
            queue.put(threadId + "\t" + "START" + "\t" + label + "\t" + timestamp);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }

    public void end(String label) {
        long timestamp = System.currentTimeMillis();
        try {
            queue.put(threadId + "\t" + "END" + "\t" + label + "\t" + timestamp);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }
}
