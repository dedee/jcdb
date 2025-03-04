package de.dedee.jcdb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class CdbWriter implements AutoCloseable {
    private final Charset charset = StandardCharsets.UTF_8;
    private final ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN); // Little-Endian buffer for
    private RandomAccessFile databaseFile = null;
    private List<CdbHashPointer> hashPointers = null;
    private int[] hashTableCounts = null;
    private int[] hashTableStartPositions = null;
    private int currentPosition = -1;
    // direct use
    private boolean finalized = false;

    /**
     * Creates a new CdbWriter and opens the specified file for writing.
     *
     * @param filepath The path to the CDB file to create
     * @throws IOException              if the file cannot be opened
     * @throws IllegalArgumentException if the filepath is null or empty
     */
    public CdbWriter(String filepath) throws IOException {
        if (filepath == null || filepath.isEmpty()) {
            throw new IllegalArgumentException("Filepath cannot be null or empty");
        }

        // Initialize data structures
        hashPointers = new ArrayList<>();
        hashTableCounts = new int[256];
        hashTableStartPositions = new int[256];

        for (int i = 0; i < 256; i++) {
            hashTableCounts[i] = 0;
        }

        try {
            databaseFile = new RandomAccessFile(filepath, "rw");
            currentPosition = 2048;
            databaseFile.seek(currentPosition);
            finalized = false;
        } catch (IOException e) {
            throw new IOException("Error opening file: " + filepath, e);
        }
    }

    /**
     * Adds a string key and value to the database.
     *
     * @param key  The key as a string
     * @param data The associated value as a string
     * @throws IOException              if an error occurs during writing
     * @throws IllegalArgumentException if key or data is null
     * @throws IllegalStateException    if the writer has already been finalized
     */
    public void add(String key, String data) throws IOException {
        add(key.getBytes(charset), data.getBytes(charset));
    }

    /**
     * Adds a binary key and value to the database.
     *
     * @param key  The key as a byte array
     * @param data The associated value as a byte array
     * @throws IOException              if an error occurs during writing
     * @throws IllegalStateException    if the writer is not initialized or has
     *                                  already been finalized
     * @throws IllegalArgumentException if key or data is null
     */
    public void add(byte[] key, byte[] data) throws IOException {
        if (finalized) {
            throw new IllegalStateException("CdbWriter has already been finalized");
        }
        // ByteBuffer is already set to little-endian
        buffer.clear();
        buffer.putInt(key.length);
        buffer.flip();
        databaseFile.write(buffer.array(), 0, 4);

        buffer.clear();
        buffer.putInt(data.length);
        buffer.flip();
        databaseFile.write(buffer.array(), 0, 4);

        databaseFile.write(key);
        databaseFile.write(data);

        int hash = CdbUtil.hash(key);
        hashPointers.add(new CdbHashPointer(hash, currentPosition));
        hashTableCounts[hash & 0xff]++;

        // Directly increment position
        currentPosition += 8;
        currentPosition += key.length;
        currentPosition += data.length;
    }

    /**
     * Finalizes the CDB file by writing the hash tables.
     * This method is automatically called by close() and normally doesn't need to
     * be used directly.
     *
     * @throws IOException if an error occurs during writing
     */
    private void finalizeDatabase() throws IOException {
        int currentEntry = 0;
        for (int i = 0; i < 256; i++) {
            currentEntry += hashTableCounts[i];
            hashTableStartPositions[i] = currentEntry;
        }

        // Safety check - if no entries exist
        if (hashPointers.isEmpty()) {
            // Create empty database
            ByteBuffer emptySlotTableBuffer = ByteBuffer.allocate(2048).order(ByteOrder.LITTLE_ENDIAN);
            databaseFile.seek(0);
            databaseFile.write(emptySlotTableBuffer.array());
            finalized = true;
            return;
        }

        CdbHashPointer[] slotPointers = new CdbHashPointer[hashPointers.size()];
        for (int i = 0; i < hashPointers.size(); i++) {
            CdbHashPointer hashPointer = hashPointers.get(i);
            if (hashPointer == null) {
                // An unexpected null entry in the list
                throw new IllegalStateException("Invalid null entry in hash pointer list at position " + i);
            }
            slotPointers[--hashTableStartPositions[hashPointer.hash & 0xff]] = hashPointer;
        }

        // Little-Endian ByteBuffer for the slot table
        ByteBuffer slotTableBuffer = ByteBuffer.allocate(2048).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < 256; i++) {
            int tableLength = hashTableCounts[i] * 2;

            // Simply use putInt - automatically stored as little-endian
            slotTableBuffer.putInt(i * 8, currentPosition);
            slotTableBuffer.putInt(i * 8 + 4, tableLength);

            int currentSlotPointer = hashTableStartPositions[i];
            CdbHashPointer[] hashTable = new CdbHashPointer[tableLength];
            for (int j = 0; j < hashTableCounts[i]; j++) {
                CdbHashPointer hashPointer = slotPointers[currentSlotPointer++];
                if (hashPointer == null) {
                    throw new IllegalStateException("Invalid null entry in slot pointer list");
                }
                int position = (hashPointer.hash >>> 8) % tableLength;
                while (hashTable[position] != null) {
                    if (++position == tableLength) {
                        position = 0;
                    }
                }
                hashTable[position] = hashPointer;
            }

            for (int j = 0; j < tableLength; j++) {
                CdbHashPointer hashPointer = hashTable[j];
                if (hashPointer != null) {
                    // Directly use ByteBuffer
                    buffer.clear();
                    buffer.putInt(hashTable[j].hash);
                    buffer.flip();
                    databaseFile.write(buffer.array(), 0, 4);

                    buffer.clear();
                    buffer.putInt(hashTable[j].position);
                    buffer.flip();
                    databaseFile.write(buffer.array(), 0, 4);
                } else {
                    // Directly use ByteBuffer
                    buffer.clear();
                    buffer.putInt(0);
                    buffer.flip();
                    databaseFile.write(buffer.array(), 0, 4);

                    buffer.clear();
                    buffer.putInt(0);
                    buffer.flip();
                    databaseFile.write(buffer.array(), 0, 4);
                }
                // Directly increment position
                currentPosition += 8;
            }
        }

        databaseFile.seek(0);
        databaseFile.write(slotTableBuffer.array());
        finalized = true;
    }

    /**
     * Closes the database by first calling finalizeDatabase()
     * and then closing the file. All resources are released.
     *
     * @throws IOException if an error occurs during writing or closing
     */
    @Override
    public void close() throws IOException {
        try {
            // If not yet finalized, do it now
            if (!finalized && databaseFile != null) {
                finalizeDatabase();
            }
        } finally {
            // Always close database, even if finalizeDatabase() fails
            if (databaseFile != null) {
                try {
                    databaseFile.close();
                } catch (IOException e) {
                    // Log error on close, but don't throw
                    System.err.println("Error closing database file: " + e.getMessage());
                } finally {
                    databaseFile = null;
                }
            }
        }
    }

    private static class CdbHashPointer {
        int hash;
        int position;

        CdbHashPointer(int hash, int position) {
            this.hash = hash;
            this.position = position;
        }
    }
}
