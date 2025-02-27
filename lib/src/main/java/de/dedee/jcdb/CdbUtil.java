package de.dedee.jcdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

final class CdbUtil {
    private static final int INITIAL_TABLE_SIZE = 2048;

    private CdbUtil() {
        // Prevent instantiation
    }

    static int[] readSlotTable(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(INITIAL_TABLE_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);
        if (channel.read(buffer, 0) != INITIAL_TABLE_SIZE) {
            throw new IOException("Corrupted database header");
        }
        buffer.flip();
        int[] table = new int[512];
        for (int i = 0; i < 256; i++) {
            table[i * 2] = buffer.getInt();
            table[i * 2 + 1] = buffer.getInt();
        }
        return table;
    }

    static int hash(byte[] key) {
        int h = 5381;
        for (int i = 0; i < key.length; i++) {
            h = (h + (h << 5)) ^ (key[i] & 0xff);
        }
        return h;
    }

}