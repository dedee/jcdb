package de.dedee.jcdb;

import com.strangegizmo.cdb.CdbMake;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class CdbReaderTests {

    private static final String TEST_CDB_PATH = "test.cdb";

    @BeforeAll
    public static void setupTestData() throws IOException {
        // Create test data file
        CdbMake cdbMake = new CdbMake();
        cdbMake.start(TEST_CDB_PATH);
        for (int i = 0; i < 100; i++) {
            cdbMake.add(("key-" + i).getBytes(), ("value-" + i).getBytes());
        }
        cdbMake.finish();
    }

    @AfterAll
    public static void cleanup() {
        // Clean up test files
        new File(TEST_CDB_PATH).delete();
    }

    @Test
    public void testGet() throws Exception {
        // Use the generated test file
        try (CdbReader cdb = CdbReader.create(Path.of(TEST_CDB_PATH))) {
            for (int i = 0; i < 100; i++) {
                assertArrayEquals(("value-" + i).getBytes(), cdb.get(("key-" + i).getBytes()));
            }
        }
    }

    @Test
    public void testWriteAndReadSimpleEntries(@TempDir Path tempDir) throws Exception {
        File cdbFile = tempDir.resolve("simple.cdb").toFile();
        
        // Write test data
        try (CdbWriter writer = new CdbWriter(cdbFile.getPath())) {
            writer.add("key1".getBytes(), "value1".getBytes());
            writer.add("key2".getBytes(), "value2".getBytes());
            writer.add("key3".getBytes(), "value3".getBytes());
        }
        
        // Read and validate
        try (CdbReader reader = CdbReader.create(cdbFile.toPath())) {
            assertArrayEquals("value1".getBytes(), reader.get("key1".getBytes()));
            assertArrayEquals("value2".getBytes(), reader.get("key2".getBytes()));
            assertArrayEquals("value3".getBytes(), reader.get("key3".getBytes()));
            assertNull(reader.get("nonexistent".getBytes()));
        }
    }
    
    @Test
    public void testWriteAndReadLargeEntries(@TempDir Path tempDir) throws Exception {
        File cdbFile = tempDir.resolve("large.cdb").toFile();
        
        // Create large test data
        byte[] largeKey = new byte[1024];   // 1KB key
        byte[] largeValue = new byte[1024 * 1024]; // 1MB value
        new Random().nextBytes(largeKey);
        new Random().nextBytes(largeValue);
        
        // Write test data
        try (CdbWriter writer = new CdbWriter(cdbFile.getPath())) {
            writer.add(largeKey, largeValue);
        }
        
        // Read and validate
        try (CdbReader reader = CdbReader.create(cdbFile.toPath())) {
            assertArrayEquals(largeValue, reader.get(largeKey));
        }
    }
    
    
    @Test
    public void testWriteAndReadHundredEntries(@TempDir Path tempDir) throws Exception {
        File cdbFile = tempDir.resolve("hundred.cdb").toFile();
        Map<String, String> testData = new HashMap<>();
        
        // Create and write 100 entries
        try (CdbWriter writer = new CdbWriter(cdbFile.getPath())) {
            for (int i = 0; i < 100; i++) {
                String key = "key" + i;
                String value = "value" + i;
                testData.put(key, value);
                writer.add(key.getBytes(), value.getBytes());
            }
        }
        
        // Read and validate all entries
        try (CdbReader reader = CdbReader.create(cdbFile.toPath())) {
            for (Map.Entry<String, String> entry : testData.entrySet()) {
                assertArrayEquals(
                    entry.getValue().getBytes(),
                    reader.get(entry.getKey().getBytes()),
                    "Error reading key: " + entry.getKey()
                );
            }
        }
    }

   
}
