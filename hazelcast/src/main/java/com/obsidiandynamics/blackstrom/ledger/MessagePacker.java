package com.obsidiandynamics.blackstrom.ledger;

import java.nio.*;

import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.model.*;

final class MessagePacker {
  private MessagePacker() {}
  
  private static final byte[] emptyBytes = {};
  
  static byte[] pack(MessageCodec codec, Message message) throws Exception {
    final String shardKey = message.getShardKey();
    final byte[] shardKeyBytes = shardKey != null ? shardKey.getBytes() : emptyBytes;
    final byte shardKeyLength = (byte) (shardKey != null ? shardKeyBytes.length : -1);
    
    final byte[] payload = codec.encode(message);
    final int payloadLength = payload.length;
    
    final int totalLength = 
        1 + shardKeyBytes.length + 
        4 + payload.length;
    final ByteBuffer buf = ByteBuffer.allocate(totalLength);
    buf.put(shardKeyLength);
    buf.put(shardKeyBytes);
    buf.putInt(payloadLength);
    buf.put(payload);
    return buf.array();
  }
  
  static class DeserializationException extends Exception {
    private static final long serialVersionUID = 1L;
    DeserializationException(String m) { super(m); }
  }
  
  static Message unpack(MessageCodec codec, byte[] bytes) throws Exception {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    
    final byte shardKeyLength = buf.get();
    final String shardKey;
    if (shardKeyLength != -1) {
      final byte[] shardKeyBytes = new byte[shardKeyLength];
      buf.get(shardKeyBytes);
      shardKey = new String(shardKeyBytes);
    } else {
      shardKey = null;
    }
    
    final int payloadLength = buf.getInt();
    final byte[] payloadBytes = new byte[payloadLength];
    buf.get(payloadBytes);
    if (buf.remaining() != 0) {
      throw new DeserializationException(String.format("%,d unread bytes remaining", buf.remaining()));
    }
    
    final Message message = codec.decode(payloadBytes);
    message.setShardKey(shardKey);
    message.setShard(0);
    return message;
  }
}
