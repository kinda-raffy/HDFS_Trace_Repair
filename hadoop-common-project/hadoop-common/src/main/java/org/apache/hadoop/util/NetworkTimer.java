package org.apache.hadoop.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class NetworkTimer {
    static final String network_path = "traffic.txt";

    public static void markOutbound(long blockId, String... metadata) {
        long timestamp = System.currentTimeMillis();
        String line = timestamp + "\toutbound\t" + blockId;
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

    public static void markInbound(long blockId) {
        long timestamp = System.currentTimeMillis();
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("traffic.txt"), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(timestamp + "\tinbound\t" + blockId);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
