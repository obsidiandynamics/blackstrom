package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public final class KryoDogSerializer extends Serializer<KryoDog> {
  @Override
  public void write(Kryo kryo, Output output, KryoDog object) {
    output.writeString(object.name);
    kryo.writeClassAndObject(output, object.friend);
  }

  @Override
  public KryoDog read(Kryo kryo, Input input, Class<KryoDog> type) {
    final String name = input.readString();
    final KryoAnimal<?> friend = (KryoAnimal<?>) kryo.readClassAndObject(input);
    return new KryoDog().named(name).withFriend(friend);
  }
}
