package com.obsidiandynamics.blackstrom.codec.kryo;

final class PayloadBuffer {
  private final byte[] bytes;

  PayloadBuffer(byte[] bytes) {
    this.bytes = bytes;
  }
  
  byte[] getBytes() {
    return bytes;
  }
}
