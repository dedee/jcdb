package de.dedee.jcdb;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Objects;

class CdbReaderImpl implements CdbReader {

    private final FileChannel channel;
    private final int[] slotTable;

    public CdbReaderImpl(FileChannel channel) throws IOException {
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
    public Iterator<byte[]> find(byte[] key) {
        return new CdbReaderResultIterator(channel, key, slotTable);
    }

    public byte[] get(byte[] key) throws IOException {
        try (CdbReaderResultIterator iterator = new CdbReaderResultIterator(channel, key, slotTable)) {
            return iterator.findNext();
        }
    }
}
 