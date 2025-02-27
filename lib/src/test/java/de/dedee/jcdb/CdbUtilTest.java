package de.dedee.jcdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class CdbUtilTest {

    @Test
    public void testHashEmptyArray() {
        byte[] empty = new byte[0];
        assertEquals(5381, CdbUtil.hash(empty));
    }

    @Test
    public void testHashSingleByte() {
        assertEquals(177604, CdbUtil.hash(new byte[]{'a'}));
    }

    @Test
    public void testHashConsistency() {
        byte[] input = "repeated test".getBytes();
        int hash1 = CdbUtil.hash(input);
        int hash2 = CdbUtil.hash(input);
        assertEquals(hash1, hash2, "Hash should be consistent for same input");
    }

    @Test
    public void testHashDifferentValues() {
        assertNotEquals(
                CdbUtil.hash("hello".getBytes()),
                CdbUtil.hash("hello!".getBytes()),
                "Different inputs should have different hashes"
        );
    }
}
