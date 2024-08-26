package org.apache.hadoop.util;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class MetricTimer implements AutoCloseable {
    private static final ThreadLocal<Deque<Long>> startTimes = ThreadLocal.withInitial(LinkedList::new);
    private static final ThreadLocal<Deque<Long>> startWallTimes = ThreadLocal.withInitial(LinkedList::new);
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private final String metric;
    private final FileWriter logFileWriter;

    public MetricTimer(String metric) {
        this.metric = metric;
        String logDir = "Benchmark";
        String logFile = String.format("%s/%s.log", logDir, metric);

        try {
            Files.createDirectories(Paths.get(logDir));
            this.logFileWriter = new FileWriter(logFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void start() {
        try {
            Deque<Long> stack = startTimes.get();
            Deque<Long> stackWT = startWallTimes.get();
            stack.push(threadMXBean.getCurrentThreadCpuTime());
            stackWT.push(System.currentTimeMillis());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void stop(String label) {
        Deque<Long> stack = startTimes.get();
        Deque<Long> stackWT = startWallTimes.get();
        if (!stack.isEmpty()) {
            long start = stack.pop();
            long startWL = stackWT.pop();
            try {
                long duration = threadMXBean.getCurrentThreadCpuTime() - start;
                long durationWL = System.currentTimeMillis() - startWL;
                writeLog(label, duration, durationWL);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void mark(String label) {
        try {
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy'T'HH:mm:ss:SSS");
            String formattedDate = sdf.format(date);
            writeLog(label, formattedDate);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void writeLog(String label, long duration, long durationWL) {
        try {
            // label, cpu time, wall time
            logFileWriter.write(String.format("%s\t%d\t%d\n", label, duration, durationWL));
            logFileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeLog(String label, String duration) {
        try {
            logFileWriter.write(duration + "\t" + label + "\n");
            logFileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        logFileWriter.flush();
    }
}
