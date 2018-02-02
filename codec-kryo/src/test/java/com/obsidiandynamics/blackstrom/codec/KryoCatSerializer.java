package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public final class KryoCatSerializer extends Serializer<KryoCat> {
  @Override
  public void write(Kryo kryo, Output output, KryoCat object) {
    output.writeString(object.name);
    kryo.writeClassAndObject(output, object.friend);
  }

  @Override
  public KryoCat read(Kryo kryo, Input input, Class<KryoCat> type) {
    final String name = input.readString();
    final KryoAnimal<?> friend = (KryoAnimal<?>) kryo.readClassAndObject(input);
    return new KryoCat().named(name).withFriend(friend);
  }
}
