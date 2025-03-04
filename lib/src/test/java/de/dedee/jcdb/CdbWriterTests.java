package de.dedee.jcdb;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;

public class CdbWriterTests {
    
    private File tempFile;
    
    @Test
    public void testBasicWriteAndRead() throws Exception {
        tempFile = File.createTempFile("test", ".cdb");
        tempFile.delete(); // We want to create the file anew
        
        // Using try-with-resources for the CdbWriter
        try (CdbWriter writer = new CdbWriter(tempFile.getAbsolutePath())) {
            writer.add("key1", "value1");
            writer.add("key2", "value2");
            // close() is called automatically
        }
        
        // Check if the file was created and is not empty
        assertTrue(tempFile.exists(), "File should exist");
        assertTrue(tempFile.length() > 0, "File should not be empty");
        
        // Read and verify with CdbReader
        try (CdbReader reader = CdbReader.create(tempFile.toPath())) {
            assertEquals("value1", reader.getString("key1"), "Should return correct value for key1");
            assertEquals("value2", reader.getString("key2"), "Should return correct value for key2");
            assertNull(reader.getString("nonexistent"), "Should return null for nonexistent key");
        }
    }
    
    @Test
    public void testEmptyDatabase() throws Exception {
        tempFile = File.createTempFile("empty", ".cdb");
        tempFile.delete();
        
        // Create empty database
        try (CdbWriter writer = new CdbWriter(tempFile.getAbsolutePath())) {
            // No entries added
            // close() is called automatically
        }
        
        assertTrue(tempFile.exists(), "File should exist");
        assertTrue(tempFile.length() > 0, "File should not be empty even for empty database");
        
        // Verify empty database
        try (CdbReader reader = CdbReader.create(tempFile.toPath())) {
            assertNull(reader.getString("any"), "Should return null for any key in empty database");
        }
    }
    
    @Test
    public void testNullValues() throws IOException {
        tempFile = File.createTempFile("null_test", ".cdb");
        tempFile.delete();
        
        try (CdbWriter writer = new CdbWriter(tempFile.getAbsolutePath())) {
            // Test null keys and values
            assertThrows(IllegalArgumentException.class, () -> {
                writer.add(null, "value");
            }, "Should throw exception for null key");
            
            assertThrows(IllegalArgumentException.class, () -> {
                writer.add("key", null);
            }, "Should throw exception for null value");
            
            assertThrows(IllegalArgumentException.class, () -> {
                writer.add((byte[])null, new byte[0]);
            }, "Should throw exception for null key");
            
            assertThrows(IllegalArgumentException.class, () -> {
                writer.add(new byte[0], null);
            }, "Should throw exception for null value");
        }
    }
    
    @Test
    public void testAddAfterClose() throws IOException {
        tempFile = File.createTempFile("add_after_close", ".cdb");
        tempFile.delete();
        
        CdbWriter writer = new CdbWriter(tempFile.getAbsolutePath());
        writer.add("key", "value");
        writer.close();
        
        // Attempt to add after close should throw exception
        assertThrows(IllegalStateException.class, () -> {
            writer.add("newKey", "newValue");
        }, "Should throw exception when adding after close");
    }
    
    @Test
    public void testMultipleFiles() throws Exception {
        // Create two separate CDB files
        File tempFile1 = File.createTempFile("test1", ".cdb");
        tempFile1.delete();
        
        File tempFile2 = File.createTempFile("test2", ".cdb");
        tempFile2.delete();
        
        // Write to first file
        try (CdbWriter writer1 = new CdbWriter(tempFile1.getAbsolutePath())) {
            writer1.add("file1key", "file1value");
        }
        
        // Write to second file
        try (CdbWriter writer2 = new CdbWriter(tempFile2.getAbsolutePath())) {
            writer2.add("file2key", "file2value");
        }
        
        // Verify first file
        try (CdbReader reader1 = CdbReader.create(tempFile1.toPath())) {
            assertEquals("file1value", reader1.getString("file1key"), "Should find key in file1");
            assertNull(reader1.getString("file2key"), "Should not find file2 key in file1");
        }
        
        // Verify second file
        try (CdbReader reader2 = CdbReader.create(tempFile2.toPath())) {
            assertEquals("file2value", reader2.getString("file2key"), "Should find key in file2");
            assertNull(reader2.getString("file1key"), "Should not find file1 key in file2");
        }
        
        // Cleanup
        tempFile1.delete();
        tempFile2.delete();
    }
    
    @Test
    public void testLargeData() throws Exception {
        tempFile = File.createTempFile("large", ".cdb");
        tempFile.delete();
        
        // Create large string data
        StringBuilder largeKey = new StringBuilder();
        StringBuilder largeValue = new StringBuilder();
        
        for (int i = 0; i < 1000; i++) {
            largeKey.append("key").append(i);
            largeValue.append("value").append(i);
        }
        
        String bigKey = largeKey.toString();
        String bigValue = largeValue.toString();
        
        try (CdbWriter writer = new CdbWriter(tempFile.getAbsolutePath())) {
            writer.add(bigKey, bigValue);
        }
        
        // Verify large data
        try (CdbReader reader = CdbReader.create(tempFile.toPath())) {
            assertEquals(bigValue, reader.getString(bigKey), "Should correctly store and retrieve large data");
        }
    }
    
    @Test
    public void testManyEntries() throws Exception {
        tempFile = File.createTempFile("many", ".cdb");
        tempFile.delete();
        
        final int COUNT = 1000;
        String[] keys = new String[COUNT];
        String[] values = new String[COUNT];
        
        // Generate unique keys and values
        for (int i = 0; i < COUNT; i++) {
            keys[i] = "key" + i;
            values[i] = "value" + i;
        }
        
        // Write all entries
        try (CdbWriter writer = new CdbWriter(tempFile.getAbsolutePath())) {
            for (int i = 0; i < COUNT; i++) {
                writer.add(keys[i], values[i]);
            }
        }
        
        // Verify all entries
        try (CdbReader reader = CdbReader.create(tempFile.toPath())) {
            for (int i = 0; i < COUNT; i++) {
                assertEquals(values[i], reader.getString(keys[i]), 
                        "Entry " + i + " should be correctly stored and retrieved");
            }
        }
    }
    
    @Test
    public void testBinaryData() throws Exception {
        tempFile = File.createTempFile("binary", ".cdb");
        tempFile.delete();
        
        // Create binary data
        byte[] binaryKey = {0x00, 0x01, 0x02, 0x03, (byte)0xFF};
        byte[] binaryValue = {0x10, 0x20, 0x30, 0x40, (byte)0xEE};
        
        try (CdbWriter writer = new CdbWriter(tempFile.getAbsolutePath())) {
            writer.add(binaryKey, binaryValue);
        }
        
        // Verify binary data
        try (CdbReader reader = CdbReader.create(tempFile.toPath())) {
            assertArrayEquals(binaryValue, reader.get(binaryKey), 
                    "Binary data should be correctly stored and retrieved");
        }
    }
}