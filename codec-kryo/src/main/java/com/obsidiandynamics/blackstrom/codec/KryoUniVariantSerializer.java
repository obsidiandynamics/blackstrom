package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public final class KryoUniVariantSerializer extends Serializer<UniVariant> {
  private static final int DEF_CONTENT_BUFFER_SIZE = 128;

  @Override
  public void write(Kryo kryo, Output output, UniVariant v) {
    output.writeString(v.getHandle().getType());
    output.writeVarInt(v.getHandle().getVersion(), true);
    
    if (v.getPacked() != null) {
      // already packed — just write out the bytes
      final var packed = mustBeSubtype(v.getPacked(), KryoPackedForm.class, 
                                       withMessage(() -> "Unsupported packed form: " + v.getPacked().getClass().getSimpleName(), 
                                                   IllegalStateException::new));
      output.writeVarInt(packed.getBytes().length, true);
      output.writeBytes(packed.getBytes());
    } else {
      // content serialization is required
      try (var contentBuffer = new Output(DEF_CONTENT_BUFFER_SIZE, -1)) {
        kryo.writeObject(contentBuffer, v.getContent());
        final var contentLength = contentBuffer.position();
        output.writeVarInt(contentLength, true);
        output.writeBytes(contentBuffer.getBuffer(), 0, contentLength);
      }
    }
  }

  @Override
  public UniVariant read(Kryo kryo, Input input, Class<? extends UniVariant> type) {
    final var contentType = input.readString();
    final var contentVersion = input.readVarInt(true);
    final var contentLength = input.readVarInt(true);
    final var contentBytes = input.readBytes(contentLength);
    return new UniVariant(new ContentHandle(contentType, contentVersion), new KryoPackedForm(contentBytes), null);
  }
}
