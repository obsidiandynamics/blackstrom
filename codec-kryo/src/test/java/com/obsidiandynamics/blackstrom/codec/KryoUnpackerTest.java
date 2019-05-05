package com.obsidiandynamics.blackstrom.codec;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import com.esotericsoftware.kryo.util.*;
import com.obsidiandynamics.format.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.zerolog.*;

public final class KryoUnpackerTest extends AbstractUnpackerTest {
  private static Pool<Kryo> createKryoPool() {
    return new Pool<>(true, false) {
      @Override
      protected Kryo create () {
        final var kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.addDefaultSerializer(Payload.class, new BasicPayloadSerializer());
        kryo.addDefaultSerializer(UniVariant.class, new KryoUniVariantSerializer());
        kryo.addDefaultSerializer(MultiVariant.class, new KryoMultiVariantSerializer());
        kryo.addDefaultSerializer(SchemaFoo_v0.class, new SchemaSerializer());
        kryo.addDefaultSerializer(SchemaFoo_v1.class, new SchemaSerializer());
        kryo.addDefaultSerializer(SchemaBar_v0.class, new SchemaSerializer());
        kryo.addDefaultSerializer(SchemaBar_v1.class, new SchemaSerializer());
        return kryo;
      }
    };
  }
  
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static void logEncoded(byte[] encoded) {
    zlg.t("encoded:\n%s", z -> z.arg(Binary.dump(encoded)));
  }
  
  private static final class BasicPayloadSerializer extends Serializer<Payload> {
    @Override
    public void write(Kryo kryo, Output output, Payload object) {
      kryo.writeClassAndObject(output, object.unpack()); 
    }

    @Override
    public Payload read(Kryo kryo, Input input, Class<? extends Payload> type) {
      return Payload.pack(kryo.readClassAndObject(input));
    }
  }
  
  private static final class SchemaSerializer extends Serializer<BaseSchema> {
    @Override
    public void write(Kryo kryo, Output output, BaseSchema object) {
      output.writeString(object.getType());
      output.writeString(object.getValue());
    }

    @Override
    public BaseSchema read(Kryo kryo, Input input, Class<? extends BaseSchema> objectClass) {
      final var type = input.readString();
      final var value = input.readString();
      return Exceptions.wrap(() -> {
        return objectClass.getDeclaredConstructor(String.class, String.class).newInstance(type, value);
      }, RuntimeException::new);
    }
  }
  
  private Pool<Kryo> kryoPool;
  
  private KryoUnpacker unpacker;

  @Override
  protected void init() {
    kryoPool = createKryoPool();
    unpacker = new KryoUnpacker(kryoPool);
  }

  @Override
  protected Unpacker<?> getUnpacker() {
    return unpacker;
  }
  
  private byte[] writeBytes(Object obj) {
    final var kryo = kryoPool.obtain();
    try {
      try (var buffer = new Output(128, -1)) {
        kryo.writeObject(buffer, obj);
        final var length = buffer.position();
        final var bytes = new byte[length];
        System.arraycopy(buffer.getBuffer(), 0, bytes, 0, length);
        return bytes;
      }
    } finally {
      kryoPool.free(kryo);
    }
  }
  
  private <T> T readBytes(byte[] bytes, Class<T> cls) {
    final var kryo = kryoPool.obtain();
    try {
      try (var buffer = new Input(bytes)) {
        return kryo.readObject(buffer, cls);
      }
    } finally {
      kryoPool.free(kryo);
    }
  }

  @Override
  protected <T> T roundTrip(T obj) {
    final var bytes = writeBytes(obj);
    logEncoded(bytes);
    return Classes.cast(readBytes(bytes, obj.getClass()));
  }
}
