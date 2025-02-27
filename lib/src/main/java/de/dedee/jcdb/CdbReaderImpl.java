package de.dedee.jcdb;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Objects;

/**
 * Implementation of {@link CdbReader} that provides read access to a Constant Database (CDB) file.
 * CDB is a fast, reliable, and simple package for creating and reading constant databases.
 */
class CdbReaderImpl implements CdbReader {

    private final FileChannel channel;
    private final int[] slotTable;

    /**
     * Creates a new CDB reader using the specified file channel.
     *
     * @param channel The file channel to read the CDB file from
     * @throws IOException If an I/O error occurs while reading the slot table
     * @throws NullPointerException If the channel is null
     */
    public CdbReaderImpl(FileChannel channel) throws IOException {
        Objects.requireNonNull(channel, "Channel cannot be null");
        this.channel = channel;
        this.slotTable = CdbUtil.readSlotTable(channel);
    }

    /**
     * Closes the underlying file channel.
     * Any I/O errors that occur during close are silently ignored.
     */
    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * Finds all values associated with the given key in the database.
     *
     * @param key The key to look up
     * @return An iterator over all values associated with the key
     */
    @Override
    public Iterator<byte[]> find(byte[] key) {
        return new CdbReaderResultIterator(channel, key, slotTable);
    }

    /**
     * Gets the first value associated with the given key in the database.
     *
     * @param key The key to look up
     * @return The first value associated with the key, or null if the key is not found
     * @throws IOException If an I/O error occurs while reading from the database
     */
    public byte[] get(byte[] key) throws IOException {
        try (CdbReaderResultIterator iterator = new CdbReaderResultIterator(channel, key, slotTable)) {
            return iterator.findNext();
        }
    }
}
 