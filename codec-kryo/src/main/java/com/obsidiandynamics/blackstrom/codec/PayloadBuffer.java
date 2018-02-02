package com.obsidiandynamics.blackstrom.codec;

final class PayloadBuffer {
  private final byte[] bytes;

  PayloadBuffer(byte[] bytes) {
    this.bytes = bytes;
  }
  
  byte[] getBytes() {
    return bytes;
  }
}
