package org.apache.hadoop.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class NetworkTimer {
    static final String network_path = "traffic.txt";

    public static void mark(long blockId, String... metadata) {
        long timestamp = System.currentTimeMillis();
        String line = timestamp + "\t" + blockId;
        if (metadata.length > 0) {
            line += "\t";
            for (String m : metadata) {
                line += m;
            }
        }
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("traffic.txt"), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);) {
            writer.write(line);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
