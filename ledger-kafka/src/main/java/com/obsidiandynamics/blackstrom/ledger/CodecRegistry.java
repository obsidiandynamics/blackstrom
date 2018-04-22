package com.obsidiandynamics.blackstrom.ledger;

import java.util.*;
import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.codec.*;

final class CodecRegistry {
  static final String CONFIG_CODEC_LOCATOR = "blackstrom.codecLocator";
  
  private static final Map<String, MessageCodec> codecs = new ConcurrentHashMap<>();
  
  private CodecRegistry() {}
  
  static String register(MessageCodec codec) {
    final String locator = UUID.randomUUID().toString();
    codecs.put(locator, codec);
    return locator;
  }
  
  static void deregister(String locator) {
    codecs.remove(locator);
  }
  
  static MessageCodec forLocator(String locator) {
    return codecs.get(locator);
  }
}
