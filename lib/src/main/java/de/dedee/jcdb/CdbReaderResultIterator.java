package de.dedee.jcdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Iterator implementation for reading multiple values associated with a key in a CDB file.
 * This class handles the internal state management for iterating through hash slots in a
 * Constant Database (CDB) file format.
 *
 * <p>CDB files store key-value pairs in a two-level hash table structure. This iterator
 * efficiently traverses through all values associated with a given key by following the
 * hash chain in the file.</p>
 *
 * <p>Usage example:</p>
 * <pre>
 * try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
 *     CdbIterator iterator = new CdbIterator(channel, key, slotTable)) {
 *     while (iterator.hasNext()) {
 *         byte[] value = iterator.next();
 *         // Process value
 *     }
 * }
 * </pre>
 */
class CdbReaderResultIterator implements Iterator<byte[]>, AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Size of each hash table entry in bytes (4 bytes for hash + 4 bytes for position)
     */
    private static final int HASH_ENTRY_SIZE = 8;

    /**
     * The file channel used to read from the CDB file
     */
    private final FileChannel channel;

    /**
     * The key being searched for in the database
     */
    private final byte[] key;

    /**
     * Array containing hash table positions and sizes (256 pairs of integers)
     */
    private final int[] slotTable;

    /**
     * The next value to be returned by this iterator
     */
    private byte[] nextValue;

    /**
     * Flag indicating whether the iterator has been initialized for searching
     */
    private boolean initialized;

    /**
     * Flag indicating whether this iterator has been closed
     */
    private boolean closed;

    /**
     * Number of slots examined in the current search (0 to hashSlots-1)
     */
    private int loop;

    /**
     * Hash value of the current key being searched
     */
    private int keyHash;

    /**
     * Total number of hash slots in the current table
     */
    private int hashSlots;

    /**
     * Starting position of the hash table in the file
     */
    private int hashPos;

    /**
     * Current position within the hash table being examined
     */
    private int keyPos;

    /**
     * Creates a new iterator for reading values associated with the specified key.
     *
     * @param channel   The FileChannel for reading from the CDB file
     * @param key       The key to search for in the database
     * @param slotTable The slot table containing hash table positions and sizes
     * @throws NullPointerException if any parameter is null
     */
    CdbReaderResultIterator(FileChannel channel, byte[] key, int[] slotTable) {
        this.channel = Objects.requireNonNull(channel, "Channel cannot be null");
        this.key = Objects.requireNonNull(key, "Key cannot be null");
        this.slotTable = Objects.requireNonNull(slotTable, "Slot table cannot be null");
    }

    /**
     * Checks if there are more values associated with the key.
     *
     * @return true if there are more values to read, false otherwise
     */
    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }

        try {
            if (!initialized) {
                loop = 0;
                nextValue = findNext();
                initialized = true;
            } else if (nextValue == null) {
                nextValue = findNext();
            }
            return nextValue != null;
        } catch (IOException e) {
            logger.error("Error reading value from CDB file", e);
            return false;
        }
    }

    /**
     * Returns the next value associated with the key.
     *
     * @return the next value as a byte array
     * @throws NoSuchElementException if there are no more values to read
     */
    @Override
    public byte[] next() {
        if (!hasNext()) {
            return null;
        }
        byte[] result = nextValue;
        nextValue = null;
        return result;
    }

    /**
     * Finds the next value associated with the key.
     *
     * @return the next matching value, or null if no more values exist
     * @throws IOException if an I/O error occurs
     */
    byte[] findNext() throws IOException {
        if (loop == 0) {
            initializeSearch();
        }
        return searchSlots();
    }

    /**
     * Initializes the search state by calculating hash positions and slots.
     * Sets up the initial position for searching the hash table.
     */
    private void initializeSearch() {
        keyHash = CdbUtil.hash(key);
        int slot = keyHash & 255;

        hashSlots = slotTable[(slot << 1) + 1];
        if (hashSlots == 0) {
            return;
        }

        hashPos = slotTable[slot << 1];
        int slotOffset = (keyHash >>> 8) % hashSlots;
        keyPos = hashPos + (slotOffset << 3);
    }

    /**
     * Searches through hash slots for matching entries.
     *
     * @return the next matching value, or null if no match is found
     * @throws IOException if an I/O error occurs
     */
    private byte[] searchSlots() throws IOException {
        while (loop < hashSlots) {
            byte[] result = readHashSlot();
            updatePosition();
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Reads and processes a single hash slot.
     *
     * @return the value if the slot contains a matching entry, null otherwise
     * @throws IOException if an I/O error occurs
     */
    private byte[] readHashSlot() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.clear();

        // NOTE: Always add position here because we do not want to synchronize channel access.
        // Otherwise, you need to take care of syncing across the threads.
        // Whereas such a read doesn't update channels pos.
        channel.read(buffer, keyPos);
        buffer.flip();
        int hash = buffer.getInt();
        int pos = buffer.getInt();
        if (pos == 0 || hash != keyHash) {
            return null;
        }

        // Read entry
        buffer.clear();
        channel.read(buffer, pos);
        buffer.flip();
        int keyLength = buffer.getInt();
        int valueLength = buffer.getInt();
        if (keyLength != key.length) {
            logger.warn("Error reading value from CDB file, key length mismatch");
            return null;
        }

        // Read key
        byte[] k = new byte[keyLength];
        channel.read(ByteBuffer.wrap(k), pos + 8);
        if (!Arrays.equals(k, key)) {
            logger.warn("Error reading value from CDB file, key mismatch");
            return null;
        }

        // Read value
        byte[] d = new byte[valueLength];
        if (channel.read(ByteBuffer.wrap(d), pos + 8 + keyLength) != valueLength) {
            logger.warn("Error reading value from CDB file, value length mismatch");
            return null;
        }
        return d;
    }

    /**
     * Updates the position counters for the next iteration.
     * Handles wrapping around to the beginning of the hash table when necessary.
     */
    private void updatePosition() {
        loop++;
        keyPos += HASH_ENTRY_SIZE;
        if (keyPos == hashPos + (hashSlots << 3)) {
            keyPos = hashPos;
        }
    }

    /**
     * Closes this iterator and releases any system resources.
     * After closing, the iterator will no longer return values.
     */
    @Override
    public void close() {
        closed = true;
        nextValue = null;
    }
} 