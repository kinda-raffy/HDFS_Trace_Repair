package org.apache.hadoop.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Timeline {
    public static void mark(String action, String label, long thread_id) {
        long timestamp = System.currentTimeMillis();
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("timeline.txt"), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(timestamp + "\t" + thread_id + "\t" + action + "\t" + label);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write the event", e);
        }
    }
}
