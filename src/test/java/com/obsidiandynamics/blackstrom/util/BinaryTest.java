package com.obsidiandynamics.blackstrom.util;

import static org.junit.Assert.*;

import java.nio.*;
import java.util.*;
import java.util.function.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.util.Binary.*;

public final class BinaryTest {
  @Test
  public void testConformance() {
    Assertions.assertUtilityClassWellDefined(Binary.class);
  }
  
  @Test
  public void testInByteRange() {
    assertFalse(Binary.isInByteRange(-0x01));
    assertTrue(Binary.isInByteRange(0x00));
    assertTrue(Binary.isInByteRange(0x01));
    assertTrue(Binary.isInByteRange(0x10));
    assertTrue(Binary.isInByteRange(0xFF));
    assertFalse(Binary.isInByteRange(0x100));
  }
  
  @Test
  public void testToBytePass() {
    assertEquals((byte) 0x00, Binary.toByte(0x00));
    assertEquals((byte) 0x01, Binary.toByte(0x01));
    assertEquals((byte) 0x10, Binary.toByte(0x10));
    assertEquals((byte) 0xFF, Binary.toByte(0xFF));
  }
  
  @Test(expected=NotAnUnsignedByteException.class)
  public void testToByteFailTooLow() {
    Binary.toByte(-0x01);
  }
  
  @Test(expected=NotAnUnsignedByteException.class)
  public void testToByteFailTooHigh() {
    Binary.toByte(0x100);
  }

  @Test
  public void testBufferToByteArray() {
    final byte[] orig = { (byte) 0x00, (byte) 0x01 };
    final ByteBuffer buf = ByteBuffer.wrap(orig);
    
    final byte[] bytes = Binary.toByteArray(buf);
    assertArrayEquals(orig, bytes);
    assertEquals(0, buf.position());
  }
  
  @Test
  public void testDump() {
    final byte[] bytes = new byte[25];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
    
    final String out = Binary.dump(bytes);
    assertEquals("00 01 02 03 04 05 06 07   08 09 0A 0B 0C 0D 0E 0F\n" + 
                 "10 11 12 13 14 15 16 17   18", out);
  }
  
  @Test
  public void testDumpByteBuffer() {
    final byte[] bytes = { (byte) 0x00, (byte) 0x01 };
    assertEquals(Binary.dump(bytes), Binary.dump(ByteBuffer.wrap(bytes)));
  }
  
  @Test
  public void testToHex() {
    assertEquals("00", Binary.toHex((byte) 0x00));
    assertEquals("0A", Binary.toHex((byte) 0x0A));
    assertEquals("10", Binary.toHex((byte) 0x10));
    assertEquals("FF", Binary.toHex((byte) 0xFF));
  }
  
  @Test
  public void testToByteArray() {
    final byte[] expected = { (byte) 0x00, (byte) 0x01, (byte) 0x10, (byte) 0xFF };
    assertArrayEquals(expected, Binary.toByteArray(0x00, 0x01, 0x10, 0xFF));
  }
  
  @Test(expected=NotAnUnsignedByteException.class)
  public void testToByteArrayFail() {
    Binary.toByteArray(0x100);
  }
  
  @Test
  public void testToByteBuffer() {
    final byte[] expected = { (byte) 0x00, (byte) 0x01, (byte) 0x10, (byte) 0xFF };
    final ByteBuffer buf = Binary.toByteBuffer(0x00, 0x01, 0x10, 0xFF);
    assertEquals(ByteBuffer.wrap(expected), buf);
  }
  
  @Test
  public void testRandomBytes() {
    final int maxAttempts = 100;
    testRandomness(maxAttempts, () -> Binary.randomBytes(1024));
  }
  
  @Test
  public void testRandomHexString() {
    final int maxAttempts = 100;
    final int length = 128;
    
    testRandomness(maxAttempts, () -> {
      final String str = Binary.randomHexString(length);
      final byte[] bytes = new byte[length / 2];
      final char[] chars = str.toCharArray();
      for (int i = 0; i < chars.length;) {
        final int lower = Integer.parseUnsignedInt(chars[i++] + "", 16);
        final int upper = Integer.parseUnsignedInt(chars[i++] + "", 16);
        bytes[i / 2 - 1] = (byte) ((upper << 4) | lower);
      }
      return bytes;
    });
  }
  
  private static boolean isWellDistributed(byte[] bytes) {
    final Set<Byte> unique = new HashSet<>(bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      unique.add(bytes[i]);
    }
    final int expectedUniqueBytes = Math.min((int) Math.round(Math.sqrt(bytes.length)), 256);
    return unique.size() >= expectedUniqueBytes;
  }
  
  private static class UnlikelyRandomSourceError extends AssertionError {
    private static final long serialVersionUID = 1L;
    UnlikelyRandomSourceError(String m) { super(m); }
  }
  
  /**
   *  Tests whether the given byte array is likely 'random' based on the distribution
   *  of unique bytes.
   *  
   *  @param maxAttempts The maximum number of failed attempts before concluding that the array
   *                     is unlikely to have come from a random source.
   *  @param randomness A source of randomness.
   */
  private static void testRandomness(int maxAttempts, Supplier<byte[]> randomness) {
    for (int i = 0; i < maxAttempts; i++) {
      final byte[] random = randomness.get();
      if (isWellDistributed(random)) {
        return;
      }
    }
    throw new UnlikelyRandomSourceError("Doesn't appear to be a random source");
  }
  
  /**
   *  Verifies that {@link #testRandomness()} works as expected.
   */
  @Test(expected=UnlikelyRandomSourceError.class)
  public void testRandomnessFailure() {
    testRandomness(1, () -> new byte[10]);
  }
}
