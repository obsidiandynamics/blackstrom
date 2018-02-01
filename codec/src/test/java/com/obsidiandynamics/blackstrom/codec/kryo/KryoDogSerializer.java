package com.obsidiandynamics.blackstrom.codec.kryo;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.obsidiandynamics.blackstrom.codec.*;

public final class KryoDogSerializer extends Serializer<Dog> {
  @Override
  public void write(Kryo kryo, Output output, Dog object) {
    output.writeString(object.name);
    kryo.writeClassAndObject(output, object.friend);
  }

  @Override
  public Dog read(Kryo kryo, Input input, Class<Dog> type) {
    final String name = input.readString();
    final Animal<?> friend = (Animal<?>) kryo.readClassAndObject(input);
    return new Dog().named(name).withFriend(friend);
  }
}
