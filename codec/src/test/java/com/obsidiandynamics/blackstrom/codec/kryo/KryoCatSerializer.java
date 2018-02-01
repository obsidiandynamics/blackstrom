package com.obsidiandynamics.blackstrom.codec.kryo;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.obsidiandynamics.blackstrom.codec.*;

public final class KryoCatSerializer extends Serializer<Cat> {
  @Override
  public void write(Kryo kryo, Output output, Cat object) {
    output.writeString(object.name);
    kryo.writeClassAndObject(output, object.friend);
  }

  @Override
  public Cat read(Kryo kryo, Input input, Class<Cat> type) {
    final String name = input.readString();
    final Animal<?> friend = (Animal<?>) kryo.readClassAndObject(input);
    return new Cat().named(name).withFriend(friend);
  }
}
