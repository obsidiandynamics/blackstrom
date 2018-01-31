package com.obsidiandynamics.blackstrom.codec.kryo;

import com.esotericsoftware.kryo.io.*;

final class KryoUtils {
  private KryoUtils() {}
  
  static void writeStringArray(Output out, String[] strings) {
    out.writeVarInt(strings.length, true);
    for (String string : strings) {
      out.writeString(string);
    }
  }
  
  static String[] readStringArray(Input in) {
    final int length = in.readVarInt(true);
    final String[] strings = new String[length];
    for (int i = 0; i < length; i++) {
      strings[i] = in.readString();
    }
    return strings;
  }
}
