package de.dedee.jcdb;

import com.strangegizmo.cdb.CdbMake;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class CdbReaderConcurrentTests {
    private static final String TEST_CDB_PATH = "test_concurrent.cdb";

    @BeforeAll
    public static void setupTestData() throws IOException, InterruptedException {
        // Create test data file
        CdbMake cdbMake = new CdbMake();
        cdbMake.start(TEST_CDB_PATH);

        // Generate a larger dataset with different patterns

        // Pattern 1: Sequential keys with single values
        for (int i = 0; i < 1000; i++) {
            cdbMake.add(("key-" + i).getBytes(), ("value-" + i).getBytes());
        }

        // Pattern 2: Keys with multiple values
        for (int i = 0; i < 100; i++) {
            String key = "multi-key-" + i;
            for (int j = 0; j < 5; j++) {
                cdbMake.add(key.getBytes(), ("multi-value-" + i + "-" + j).getBytes());
            }
        }

        // Pattern 3: Long keys and values
        for (int i = 0; i < 50; i++) {
            String longKey = "long-key-" + "x".repeat(100) + "-" + i;
            String longValue = "long-value-" + "y".repeat(1000) + "-" + i;
            cdbMake.add(longKey.getBytes(), longValue.getBytes());
        }

        // Pattern 4: Keep some of the original test keys for backwards compatibility
        cdbMake.add("test-key-0".getBytes(), "value-0".getBytes());
        cdbMake.add("test-key-1".getBytes(), "value-1a".getBytes());
        cdbMake.add("test-key-1".getBytes(), "value-1b".getBytes());
        cdbMake.add("test-key-2".getBytes(), "value-2".getBytes());

        cdbMake.finish();
    }

    @AfterAll
    public static void cleanup() {
        // Clean up test files
        new File(TEST_CDB_PATH).delete();
    }

    @Test
    public void testConcurrentFind() throws Exception {
        // Use the generated test file
        try (CdbReader cdb = CdbReader.create(Path.of(TEST_CDB_PATH))) {

            int numThreads = 200;
            int queriesPerThread = 100;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Future<Boolean>> futures = new ArrayList<>();

            try {
                // Submit tasks for concurrent execution
                for (int i = 0; i < numThreads; i++) {
                    futures.add(executor.submit(() -> {
                        try {
                            for (int j = 0; j < queriesPerThread; j++) {
                                // Example key to look up - modify based on your test data
                                byte[] key = ("key-" + j).getBytes();
                                byte[] expectedValue = ("value-" + j).getBytes();

                                // Check direct get
                                assertArrayEquals(expectedValue, cdb.get(key), "In thread we detected collision");

                                // Just iterate through results to ensure no concurrent modification issues
                                Iterator<byte[]> results = cdb.find(key);
                                // Consume the iterator to ensure no concurrent modification issues
                                if (!results.hasNext()) {
                                    return false;
                                }
                                if (!Arrays.equals(expectedValue, results.next())) {
                                    return false;
                                }
                                if (results.hasNext()) {
                                    return false;
                                }
                            }
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    }));
                }

                // Wait for all threads to complete and check results
                for (Future<Boolean> future : futures) {
                    assertTrue(future.get(30, TimeUnit.SECONDS), "Concurrent find operation failed");
                }
            } finally {
                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.MINUTES);
                cdb.close();
            }
        }
    }

    @Test
    public void testParallelStreamFind() throws Exception {
        // Use the generated test file

        try (CdbReader cdb = CdbReader.create(Path.of(TEST_CDB_PATH))) {
            int numQueries = 10_000;

            boolean allSuccessful = IntStream.range(0, numQueries)
                    .parallel()
                    .mapToObj(j -> {
                        try {
                            byte[] key = ("key-" + j % 1000).getBytes();
                            byte[] expectedValue = ("value-" + j % 1000).getBytes();
                            Iterator<byte[]> results = cdb.find(key);

                            // Consume the iterator to ensure no concurrent modification issues
                            if (results.hasNext()) {
                                if (Arrays.equals(expectedValue, results.next())) {
                                    return !results.hasNext();
                                }
                            }
                            return false;
                        } catch (Exception e) {
                            return false;
                        }
                    })
                    .allMatch(success -> success);

            assertTrue(allSuccessful, "Parallel stream operations completed successfully");
        }
    }

    @Test
    public void testConcurrentGet() throws Exception {
        // Use the generated test file
        try (CdbReader cdb = CdbReader.create(Path.of(TEST_CDB_PATH))) {

            int numThreads = 200;
            int queriesPerThread = 100;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Future<Boolean>> futures = new ArrayList<>();

            try {
                // Submit tasks for concurrent execution
                for (int i = 0; i < numThreads; i++) {
                    futures.add(executor.submit(() -> {
                        try {
                            for (int j = 0; j < queriesPerThread; j++) {
                                // Example key to look up - modify based on your test data
                                byte[] key = ("key-" + j).getBytes();
                                byte[] expectedValue = ("value-" + j).getBytes();

                                // Check direct get
                                assertArrayEquals(expectedValue, cdb.get(key), "In thread we detected collision");
                            }
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    }));
                }

                // Wait for all threads to complete and check results
                for (Future<Boolean> future : futures) {
                    assertTrue(future.get(30, TimeUnit.SECONDS), "Concurrent get operation failed");
                }
            } finally {
                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.MINUTES);
                cdb.close();
            }
        }
    }

    @Test
    public void testNonExistentKeys() throws Exception {
        // Use the generated test file
        try (CdbReader cdb = CdbReader.create(Path.of(TEST_CDB_PATH))) {

            int numThreads = 50;
            int queriesPerThread = 20;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Future<Boolean>> futures = new ArrayList<>();

            try {
                // Submit tasks for concurrent execution
                for (int i = 0; i < numThreads; i++) {
                    futures.add(executor.submit(() -> {
                        try {
                            for (int j = 0; j < queriesPerThread; j++) {
                                // Example non-existent key to look up
                                byte[] key = ("non-existent-key-" + j).getBytes();

                                // Check direct get
                                assertNull(cdb.get(key), "Expected null for non-existent key");

                                // Check find method
                                Iterator<byte[]> results = cdb.find(key);
                                assertFalse(results.hasNext(), "Expected no results for non-existent key");
                            }
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    }));
                }

                // Wait for all threads to complete and check results
                for (Future<Boolean> future : futures) {
                    assertTrue(future.get(30, TimeUnit.SECONDS), "Non-existent key operation failed");
                }
            } finally {
                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.MINUTES);
                cdb.close();
            }
        }
    }
}