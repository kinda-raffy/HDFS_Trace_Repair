package org.apache.hadoop.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class TRTraceSnapshot {
    public static void mark(String where, int idx, byte[] data) {
        String fileName = where + "-" + idx + ".txt";
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);) {
            writer.write(Arrays.toString(data));
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
