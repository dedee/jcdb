package de.dedee.jcdb;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * Interface for reading Constant Database (CDB) files.
 * CDB is a fast, reliable, and simple package for creating and reading constant databases,
 * which are key-value pairs stored in a single file.
 */
public interface CdbReader extends AutoCloseable {

    /**
     * Gets the first value associated with the given key in the database.
     *
     * @param key the byte array representing the key to look up
     * @return the first value associated with the key, or null if the key is not found
     * @throws IOException if an I/O error occurs while reading the database
     */
    byte[] get(byte[] key) throws IOException;

    /**
     * Finds all values associated with the given key in the database.
     *
     * @param key the byte array representing the key to look up
     * @return an Iterator of byte arrays containing all values associated with the key
     * @throws IOException if an I/O error occurs while reading the database
     */
    Iterator<byte[]> find(byte[] key) throws IOException;

    /**
     * Factory method to create a new CdbReader instance from a file path.
     *
     * @param path the path to the CDB file
     * @return a new CdbReader instance
     * @throws IOException if an I/O error occurs while opening or reading the file
     */
    static CdbReader create(Path path) throws IOException {
        return new CdbReaderImpl(FileChannel.open(path, StandardOpenOption.READ));
    }
} 