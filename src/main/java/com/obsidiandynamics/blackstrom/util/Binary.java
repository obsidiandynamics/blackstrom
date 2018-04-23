package com.obsidiandynamics.blackstrom.util;

import java.nio.*;
import java.util.*;

/**
 *  Provides conversion and printing utilities for binary data (byte arrays).
 */
public final class Binary {
  private Binary() {}
  
  /**
   *  Verifies whether the given {@code int} lies in the allowable unsigned byte range 
   *  (0x00—0xFF).
   *  
   *  @param intToTest The number to test.
   *  @return True if the number lies in the unsigned byte range.
   */
  public static boolean isInByteRange(int intToTest) {
    return intToTest >= 0x00 && intToTest <= 0xFF;
  }
  
  public static final class NotAnUnsignedByteException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    NotAnUnsignedByteException(String m) { super(m); }
  }
  
  /**
   *  Ensures that the given {@code int} lies in the allowable unsigned byte range 
   *  (0x00—0xFF), returning the byte value if this is the case, and throwing a 
   *  {@link NotAnUnsignedByteException} otherwise.
   *  
   *  @param intToTest The number to test.
   *  @return The byte value.
   */
  public static byte toByte(int intToTest) {
    if (! isInByteRange(intToTest)) throw new NotAnUnsignedByteException("Not in unsigned byte range " + intToTest);
    return (byte) intToTest;
  }
  
  /**
   *  Converts a given {@link ByteBuffer} to a byte array.
   *  
   *  @param buf The buffer to convert.
   *  @return The resulting byte array.
   */
  public static byte[] toByteArray(ByteBuffer buf) {
    final int pos = buf.position();
    final byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    buf.position(pos);
    return bytes;
  }
  
  /**
   *  A variant of {@link #dump} that works on a {@link ByteBuffer}.
   *  
   *  @param buf The buffer.
   *  @return The formatted string, potentially containing newline characters.
   */
  public static String dump(ByteBuffer buf) {
    return dump(toByteArray(buf));
  }
  
  /**
   *  Dumps the contents of the given byte array to a formatted hex string, using a multi-line,
   *  8 + 8 layout commonly used in hex editors.<p>
   *  
   *  A typical hex dump resembles the following:<br>
   *  {@code
   *  20 00 15 73 6F 6D 65 2F   74 6F 70 69 63 2F 74 6F
   *  2F 70 75 62 6C 69 73 68   00 01 02
   *  }
   *  
   *  @param bytes The byte array.
   *  @return The formatted string, potentially containing newline characters.
   */
  public static String dump(byte[] bytes) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      sb.append(toHex(bytes[i]));
      if (i != bytes.length - 1) {
        if (i % 16 == 15) {
          sb.append('\n');
        } else if (i % 8 == 7) {
          sb.append("   ");
        } else {
          sb.append(' ');
        }
      }
    }
    return sb.toString();
  }
  
  /**
   *  Converts a given byte to a pair of hex characters, zero-padded if
   *  the value is lower than 0x10.
   *  
   *  @param b The byte to convert.
   *  @return The hex string.
   */
  public static String toHex(byte b) {
    final String str = Integer.toHexString(Byte.toUnsignedInt(b)).toUpperCase();
    return str.length() < 2 ? "0" + str : str;
  }
  
  /**
   *  Converts a varargs array of integers into a byte array, where each of the
   *  integers is assumed to be holding an unsigned byte value.
   *  
   *  @param unsignedBytes The bytes (valid values 0x00—0xFF) to convert.
   *  @return The resulting byte array.
   */
  public static byte[] toByteArray(int... unsignedBytes) {
    final byte[] bytes = new byte[unsignedBytes.length];
    for (int i = 0; i < unsignedBytes.length; i++) {
      bytes[i] = toByte(unsignedBytes[i]);
    }
    return bytes;
  }
  
  /**
   *  Converts a varargs array of integers into a {@link ByteBuffer}, where the integers are
   *  assumed to be holding an unsigned byte value.
   *  
   *  @param unsignedBytes The bytes (valid values 0x00—0xFF) to convert.
   *  @return The resulting {@link ByteBuffer}.
   */
  public static ByteBuffer toByteBuffer(int... unsignedBytes) {
    final byte[] bytes = toByteArray(unsignedBytes);
    return ByteBuffer.wrap(bytes);
  }
  
  /**
   *  Produces an array of random bytes.
   *  
   *  @param length The length of the array.
   *  @return The random byte array.
   */
  public static byte[] randomBytes(int length) {
    final byte[] bytes = new byte[length];
    new Random().nextBytes(bytes);
    return bytes;
  }
  
  /**
   *  Produces a random hex string, where each character is between '0' and 'F'.
   *  
   *  @param length The length of the string.
   *  @return The random hex string.
   */
  public static String randomHexString(int length) {
    final StringBuilder sb = new StringBuilder(length);
    final SplittableRandom random = new SplittableRandom();
    for (int i = 0; i < length; i++) {
      sb.append(Integer.toHexString(random.nextInt(0x10)).toUpperCase());
    }
    return sb.toString();
  }
}