package de.dedee.jcdb;

import com.strangegizmo.cdb.CdbMake;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class CdbReaderTests {

    private static final String TEST_CDB_PATH = "test.cdb";

    @BeforeAll
    public static void setupTestData() throws IOException, InterruptedException {
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
    public void tesGet() throws Exception {
        // Use the generated test file
        try (CdbReader cdb = CdbReader.create(Path.of(TEST_CDB_PATH))) {
            for (int i = 0; i < 100; i++) {
                assertArrayEquals(("value-" + i).getBytes(), cdb.get(("key-" + i).getBytes()));
            }
        }
    }
}
