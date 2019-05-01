package com.obsidiandynamics.blackstrom.codec;

public interface Unpacker<P extends PackedForm> {
  Class<? extends P> getPackedType();
  
  Object unpack(P packed, Class<?> contentClass);
}
