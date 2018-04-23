package com.obsidiandynamics.blackstrom.util;

import static org.junit.Assert.*;

import java.nio.*;
import java.util.*;
import java.util.function.*;

import org.junit.*;
import org.junit.runners.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.util.Binary.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class BinaryTest {
  private static final boolean PRINT_DUMPS = false;
  
  private static void printDump(String dump) {
    if (PRINT_DUMPS) System.out.println("---\n" + dump + "\n---");
  }
  
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
  
  private static String pad(String str) {
    return String.format("%-52s", str);
  }
  
  @Test
  public void testDumpBlank() {
    final byte[] bytes = new byte[0];
    
    final String out = Binary.dump(bytes);
    assertEquals("", out);
  }
  
  @Test
  public void testDumpTiny() {
    final byte[] bytes = new byte[1];
    final int initial = 30;
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = Binary.toByte(i + initial);
    }
    
    final String out = Binary.dump(bytes);
    printDump(out);
    assertEquals(pad("1E") + ".", out);
  }
  
  @Test
  public void testDumpSmall() {
    final byte[] bytes = new byte[9];
    final int initial = 40;
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = Binary.toByte(i + initial);
    }
    
    final String out = Binary.dump(bytes);
    printDump(out);
    assertEquals(pad("28 29 2A 2B 2C 2D 2E 2F   30") + "()*+,-./0", out);
  }
  
  @Test
  public void testDumpMedium() {
    final byte[] bytes = new byte[17];
    final int initial = 50;
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = Binary.toByte(i + initial);
    }
    
    final String out = Binary.dump(bytes);
    printDump(out);
    assertEquals(pad("32 33 34 35 36 37 38 39   3A 3B 3C 3D 3E 3F 40 41") + "23456789:;<=>?@A" + "\n" + 
                 pad("42") + "B", out);
  }
  
  @Test
  public void testDumpLarge() {
    final byte[] bytes = new byte[25];
    final int initial = 60;
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = Binary.toByte(i + initial);
    }
    
    final String out = Binary.dump(bytes);
    printDump(out);
    assertEquals(pad("3C 3D 3E 3F 40 41 42 43   44 45 46 47 48 49 4A 4B") + "<=>?@ABCDEFGHIJK" + "\n" +
                 pad("4C 4D 4E 4F 50 51 52 53   54") + "LMNOPQRST", out);
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
