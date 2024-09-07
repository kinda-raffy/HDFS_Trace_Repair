package org.apache.hadoop.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class NetworkTimer {
    static final String network_path = "traffic.txt";

    public static void markOutbound(long blockId) {
        long timestamp = System.currentTimeMillis();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(network_path, true))) {
            writer.write(timestamp + "\toutbound\t" + blockId);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void markInbound(long blockId) {
        long timestamp = System.currentTimeMillis();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(network_path, true))) {
            writer.write(timestamp + "\tinbound\t" + blockId);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
