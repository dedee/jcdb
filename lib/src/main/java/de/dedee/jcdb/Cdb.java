package de.dedee.jcdb;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;

class Cdb implements CdbReader {
   
    private final FileChannel channel;
    private final int[] slotTable;

    public Cdb(FileChannel channel) throws IOException {
        Objects.requireNonNull(channel, "Channel cannot be null");
        this.channel = channel;
        this.slotTable = CdbUtil.readSlotTable(channel);
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    public Iterator<byte[]> find(byte[] key)  {
        return new CdbIterator(channel, key, slotTable);
    }

    public byte[] get(byte[] key) throws IOException {
        try (CdbIterator iterator = new CdbIterator(channel, key, slotTable)) {
            return iterator.findNext();
        }
    }
}
 